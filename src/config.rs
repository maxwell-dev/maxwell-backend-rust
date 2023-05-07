use std::num::NonZeroUsize;

use anyhow::{Context, Result};
use once_cell::sync::Lazy;

#[derive(Debug, Deserialize)]
pub struct Config {
  pub http_port: u32,
  pub master_endpoints: Vec<String>,
  #[serde(default = "available_parallelism")]
  pub puller_number: u16,
  pub db: DbConfig,
}

#[derive(Debug, Deserialize)]
pub struct DbConfig {
  pub path: String,
  pub seriesdb: SeriesdbConfig,
}

#[derive(Debug, Deserialize)]
pub struct SeriesdbConfig {
  pub table_cache_num_shard_bits: i32,
  pub write_buffer_size: usize,
  pub max_write_buffer_number: i32,
  pub min_write_buffer_number_to_merge: i32,
  pub max_bytes_for_level_base: u64,
  pub max_bytes_for_level_multiplier: f64,
  pub target_file_size_base: u64,
  pub target_file_size_multiplier: i32,
  pub level_zero_file_num_compaction_trigger: i32,
  pub max_background_jobs: i32,
}

fn available_parallelism() -> u16 {
  let n = std::thread::available_parallelism()
    .unwrap_or_else(|_err| NonZeroUsize::new(8 as usize).unwrap())
    .get();
  if n >= std::u16::MAX as usize {
    std::u16::MAX
  } else {
    n as u16
  }
}

impl Config {
  pub(crate) fn new(path: &str) -> Result<Self> {
    Ok(
      config::Config::builder()
        .add_source(config::File::with_name(path))
        .build()
        .with_context(|| format!("Failed to read config from: {:?}", path))?
        .try_deserialize()
        .with_context(|| format!("Failed to deserialize config from: {:?}", path))?,
    )
  }
}

pub static CONFIG: Lazy<Config> = Lazy::new(|| Config::new("config/config.toml").unwrap());
