use anyhow::Result;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{HashMap, BTreeMap};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Preset {
    pub name: String,
    #[serde(serialize_with = "serialize_ordered_map")]
    // sound_id -> volume (if present, sound is active at this volume)
    pub sounds: HashMap<String, f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PresetsConfig {
    #[serde(default)]
    pub presets: Vec<Preset>,
}

impl PresetsConfig {
    pub fn load() -> Result<Self> {
        let path = get_presets_path()?;
        if path.exists() {
            let content = fs::read_to_string(&path)?;
            let config: PresetsConfig = toml::from_str(&content)?;
            Ok(config)
        } else {
            Ok(Self::default())
        }
    }

    pub fn save(&self) -> Result<()> {
        let path = get_presets_path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}

fn get_presets_path() -> Result<PathBuf> {
    if let Some(proj_dirs) = ProjectDirs::from("com", "tanin", "tanin") {
        Ok(proj_dirs.config_dir().join("presets.toml"))
    } else {
        Ok(PathBuf::from("presets.toml"))
    }
}

fn serialize_ordered_map<S>(
    map: &HashMap<String, f32>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let ordered: BTreeMap<_, _> = map.iter().collect();
    ordered.serialize(serializer)
}

