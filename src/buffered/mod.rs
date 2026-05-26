//! Streaming audio architecture with gapless loop prefetching.

pub mod source;
pub mod worker;

pub use worker::{init_worker_pool, spawn_stream, DecodeTask};

#[cfg(test)]
mod tests {
    use super::worker::*;
    use rodio::Source;
    use std::sync::mpsc::channel;
    use std::time::Duration;
    // 移除了不必要的 Arc import

    // ==========================================
    // 1. 構造一個虛擬的音訊源 (Mock Source)
    // ==========================================
    struct MockSource {
        samples_left: usize,
    }

    impl Iterator for MockSource {
        type Item = f32;
        fn next(&mut self) -> Option<Self::Item> {
            if self.samples_left > 0 {
                self.samples_left -= 1;
                Some(0.1)
            } else {
                None
            }
        }
    }

    impl Source for MockSource {
        fn current_frame_len(&self) -> Option<usize> { None }
        fn channels(&self) -> u16 { 2 }
        fn sample_rate(&self) -> u32 { 44100 }
        fn total_duration(&self) -> Option<Duration> { None }
    }

    // ==========================================
    // 測試 1：Actor 狀態機的完美掛起與喚醒
    // ==========================================
    #[test]
    fn test_actor_suspend_and_resume() {
        let (global_tx, global_rx) = channel();

        // 🚨 修正點：
        // 1. 第一個參數傳遞 &global_tx 參考
        // 2. 第二個參數傳遞閉包 || Ok(Box::new(...))
        // 3. 加上 .unwrap() 解開 Result
        let mut buffered_source = spawn_stream(
            &global_tx,
            || -> Result<Box<dyn Source<Item = f32> + Send>, anyhow::Error> {
                Ok(Box::new(MockSource { samples_left: 44100 * 2 * 10 }))
            }
        ).unwrap();

        let task = global_rx.try_recv().expect("初始化後，Task 應該在全域佇列中排隊");

        let mut worker_task = task;
        loop {
            worker_task.process_chunk();
            match global_rx.try_recv() {
                Ok(returned_task) => worker_task = returned_task,
                Err(_) => break,
            }
        }

        {
            let suspended = buffered_source.suspended_task.lock().unwrap();
            assert!(suspended.is_some(), "快取滿了，Task 必須處於掛起 (Suspended) 狀態！");
        }

        let mock_chunk_size = 44100 * 2;
        for _ in 0..mock_chunk_size {
            let _ = buffered_source.next();
        }

        let _ = global_rx.try_recv().expect("前台吐出空水桶後，必須喚醒 Task 並重新排隊！");

        {
            let suspended = buffered_source.suspended_task.lock().unwrap();
            assert!(suspended.is_none(), "Task 被喚醒後，休息室必須是空的！");
        }
    }

    // ==========================================
    // 測試 2：Weak 指標防禦記憶體洩漏與優雅退出
    // ==========================================
    #[test]
    fn test_drop_cleanup_no_leak() {
        let (global_tx, global_rx) = channel();

        let task_to_test = {
            // 🚨 同樣修正呼叫方式
            let _buffered_source = spawn_stream(
                &global_tx,
                || -> Result<Box<dyn Source<Item = f32> + Send>, anyhow::Error> {
                    Ok(Box::new(MockSource { samples_left: 44100 * 2 * 10 }))
                }
            ).unwrap();

            global_rx.try_recv().expect("Task 應該在排隊")
        }; // <--- _buffered_source 在這裡被 drop 銷毀

        task_to_test.process_chunk();

        assert!(
            global_rx.try_recv().is_err(),
            "前台銷毀後，Task 必須自然死亡，絕不能產生殭屍任務重新排隊！"
        );
    }

    // ==========================================
    // 測試 3：音訊正常結束 (EOF - End of File)
    // ==========================================
    #[test]
    fn test_audio_eof_graceful_shutdown() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let (global_tx, global_rx) = channel();

        let extremely_short_source_len = 44100;

        // 使用原子變數來記住這是第幾次呼叫 factory
        let is_first_run = std::sync::Arc::new(AtomicBool::new(true));

        let mut buffered_source = spawn_stream(
            &global_tx,
            move || -> Result<Box<dyn Source<Item = f32> + Send>, anyhow::Error> {
                // 第一次呼叫 (初始化時)：給它一首短歌
                if is_first_run.swap(false, Ordering::SeqCst) {
                    Ok(Box::new(MockSource { samples_left: extremely_short_source_len }))
                } else {
                    // 第二次呼叫 (Gapless 想接下一首時)：拒絕它！模擬播放清單結束
                    Err(anyhow::anyhow!("EOF: 播放清單結束，沒有下一首了"))
                }
            }
        ).unwrap();

        let task = global_rx.try_recv().expect("Task 應該在排隊");
        let mut worker_task = task;

        loop {
            worker_task.process_chunk();
            match global_rx.try_recv() {
                Ok(returned_task) => worker_task = returned_task,
                Err(_) => break, // 任務不再排隊了
            }
        }

        // 1. 驗證 Worker 行為：因為第二次 factory 回傳 Err，Task 必須銷毀！
        {
            let suspended = buffered_source.suspended_task.lock().unwrap();
            assert!(suspended.is_none(), "歌曲結束時，Task 必須銷毀，絕不能在休息室睡覺！");
        }

        // 2. 驗證前台行為
        let mut consumed_samples = 0;
        for _ in 0..(extremely_short_source_len * 2) {
            match buffered_source.next() {
                Some(_) => consumed_samples += 1,
                None => break,
            }
        }

        // 3. 驗證：前台精準地拿到了那半桶水！
        assert_eq!(
            consumed_samples, extremely_short_source_len,
            "前台讀取的樣本數必須與歌曲總長度完全一致！"
        );
    }

    // ==========================================
    // 測試 4：解碼器崩潰兜底 (Panic Recovery)
    // 驗證當底層 C 庫或解碼器發生 Panic 時，Worker 能否安全下班
    // ==========================================
    #[test]
    fn test_decoder_panic_recovery() {
        // 建立一個會「延遲引爆」的惡意 Source
        struct PanicSource {
            samples_yielded: usize,
        }
        impl Iterator for PanicSource {
            type Item = f32;
            fn next(&mut self) -> Option<Self::Item> {
                // 假裝正常，放出前 10000 個樣本，讓 spawn_stream 的 100ms (8820) 預讀能順利過關
                if self.samples_yielded < 10000 {
                    self.samples_yielded += 1;
                    Some(0.0)
                } else {
                    // 等 Worker 接手後，突然引爆！
                    panic!("Simulated decoder panic (Corrupted audio frame)!");
                }
            }
        }
        impl Source for PanicSource {
            fn current_frame_len(&self) -> Option<usize> { None }
            fn channels(&self) -> u16 { 2 }
            fn sample_rate(&self) -> u32 { 44100 }
            fn total_duration(&self) -> Option<std::time::Duration> { None }
        }

        let (global_tx, global_rx) = channel();

        let mut buffered_source = spawn_stream(
            &global_tx,
            || -> Result<Box<dyn Source<Item = f32> + Send>, anyhow::Error> {
                // 初始化為 0
                Ok(Box::new(PanicSource { samples_yielded: 0 }))
            }
        ).unwrap();

        let task = global_rx.try_recv().expect("Task 應該在排隊");

        // 這一次，炸彈將在 Worker 內部精準引爆！
        task.process_chunk();

        // 1. 驗證 Worker 行為：
        assert!(
            global_rx.try_recv().is_err(),
            "解碼器崩潰後，Task 必須銷毀，絕不能重新排隊！"
        );

        // 2. 驗證前台行為：前台應該能把崩潰前預讀的「安全資料」播完，然後優雅地收到 None
        let mut safe_samples_played = 0;
        while let Some(_) = buffered_source.next() {
            safe_samples_played += 1;
        }

        // 驗證它真的播了剛才的預讀資料
        assert!(
            safe_samples_played > 0,
            "前台應該要能播放崩潰前預讀的安全資料！"
        );
    }

    // ==========================================
    // 測試 5：首播瞬間失敗 (Initialization Failure)
    // 驗證當完全沒有合法音訊可讀時，系統不會死鎖
    // ==========================================
    #[test]
    fn test_initialization_failure() {
        let (global_tx, global_rx) = channel();
        
        // 模擬 factory 連第一首歌都交不出來 (例如: 找不到檔案)
        let result = spawn_stream(
            &global_tx,
            || -> Result<Box<dyn Source<Item = f32> + Send>, anyhow::Error> {
                Err(anyhow::anyhow!("Permission denied: /path/to/music.opus"))
            }
        );

        // 這裡有兩種可能的正確結果，取決於你 spawn_stream 的實作細節：
        
        // 情況 A：如果你在 spawn_stream 內部「立刻」呼叫了 factory 來驗證
        if result.is_err() {
            // 完美！建構子直接攔截了錯誤，系統乾淨俐落
            assert!(global_rx.try_recv().is_err(), "建構失敗不應該產生任何 Task");
        } 
        // 情況 B：如果你把第一次呼叫 factory 的動作推遲到了 Worker 裡 (Lazy initialization)
        else if let Ok(mut buffered_source) = result {
            let task = global_rx.try_recv().expect("Lazy Task 排隊中");
            task.process_chunk(); // Worker 嘗試初始化，但失敗了
            
            // Worker 失敗後必須銷毀自己
            assert!(global_rx.try_recv().is_err(), "初始化失敗，Task 必須銷毀");
            
            // 前台等不到任何資料，必須回傳 None
            assert!(
                buffered_source.next().is_none(),
                "前台必須安全地拿到 None，不能死鎖等待！"
            );
        }
    }
}

