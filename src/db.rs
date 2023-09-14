use std::borrow::Borrow;

use anyhow::{Context, Result};
use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BytesMut};
use once_cell::sync::Lazy;
use seriesdb::prelude::{Coder, Options, TtlDb};

use crate::config::*;

pub(crate) fn open_db(db_config: &DbConfig) -> Result<TtlDb> {
  Ok(
    TtlDb::open(&db_config.path, db_config.ttl, &mut build_options(&db_config.seriesdb))
      .with_context(|| format!("Failed to open db from: {:?}", db_config.path))?,
  )
}

pub struct MsgCoder;

impl Coder<u64, Bytes> for MsgCoder {
  type EncodedKey = [u8; 8];
  type EncodedValue = Bytes;

  #[inline(always)]
  fn encode_key<K: Borrow<u64>>(key: K) -> Self::EncodedKey {
    let mut buf = [0; 8];
    BigEndian::write_u64(&mut buf, *key.borrow());
    buf
  }

  #[inline(always)]
  fn decode_key(key: &[u8]) -> u64 {
    BigEndian::read_u64(key)
  }

  #[inline(always)]
  fn encode_value<V: Borrow<Bytes>>(value: V) -> Self::EncodedValue {
    value.borrow().clone()
  }

  #[inline(always)]
  fn decode_value(value: &[u8]) -> Bytes {
    BytesMut::from(value).freeze()
  }
}

fn build_options(seriesdb_config: &SeriesdbConfig) -> Options {
  let mut options: Options = Options::new();
  options.set_level_zero_file_num_compaction_trigger(
    seriesdb_config.level_zero_file_num_compaction_trigger,
  );
  options.set_max_background_jobs(seriesdb_config.max_background_jobs);
  options.set_max_bytes_for_level_base(seriesdb_config.max_bytes_for_level_base);
  options.set_max_bytes_for_level_multiplier(seriesdb_config.max_bytes_for_level_multiplier);
  options.set_max_write_buffer_number(seriesdb_config.max_write_buffer_number);
  options.set_min_write_buffer_number_to_merge(seriesdb_config.min_write_buffer_number_to_merge);
  options.set_table_cache_num_shard_bits(seriesdb_config.table_cache_num_shard_bits);
  options.set_target_file_size_base(seriesdb_config.target_file_size_base);
  options.set_target_file_size_multiplier(seriesdb_config.target_file_size_multiplier);
  options.set_write_buffer_size(seriesdb_config.write_buffer_size);
  options
}

pub static DB: Lazy<TtlDb> = Lazy::new(|| open_db(&CONFIG.db).unwrap());
