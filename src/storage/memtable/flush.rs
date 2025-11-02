use super::table::{Entry, MemTable};
use crate::storage::sstable::builder::SsTableBuilder;
use crate::storage::sstable::TableId;
use std::path::Path;

pub struct FlushResult {
    pub id: TableId,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
    pub file_len: u64,
}

pub fn flush_memtable_to_sstable(
    mem: MemTable,
    tmp_path: &Path,
    block_size: usize,
) -> std::io::Result<FlushResult> {
    let mut builder = SsTableBuilder::new(tmp_path, block_size);
    let mut smallest: Option<Vec<u8>> = None;
    let mut largest: Option<Vec<u8>> = None;
    for (k, v) in mem.iter() {
        if smallest.is_none() {
            smallest = Some(k.clone());
        }
        largest = Some(k.clone());
        match v {
            Entry::Put(val) => builder.add_put(k, val),
            Entry::Delete => builder.add_delete(k),
        }
    }
    let (id, _index_handle) = builder.finish()?;
    let meta = std::fs::metadata(tmp_path)?;
    Ok(FlushResult {
        id,
        smallest: smallest.unwrap_or_default(),
        largest: largest.unwrap_or_default(),
        file_len: meta.len(),
    })
}
