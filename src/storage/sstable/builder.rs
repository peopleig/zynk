use super::{BlockHandle, TableId};
use crate::storage::sstable::{
    block::DataBlock, index::Index, FOOTER_SIZE, SSTABLE_MAGIC, SSTABLE_VERSION,
};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

pub struct SsTableBuilder {
    file: File,
    block: DataBlock,
    block_size: usize,
    index: Index,
    last_key_in_block: Vec<u8>,
}

impl SsTableBuilder {
    pub fn new(tmp_path: &Path, block_size: usize) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(tmp_path)
            .expect("open tmp sstable");
        Self {
            file,
            block: DataBlock::new(block_size),
            block_size,
            index: Index::new(),
            last_key_in_block: Vec::new(),
        }
    }

    pub fn add_put(&mut self, key: &[u8], value: &[u8]) {
        if self.block.is_full() {
            self.flush_block();
        }
        self.block.add_put(key, value);
        self.last_key_in_block.clear();
        self.last_key_in_block.extend_from_slice(key);
    }

    pub fn add_delete(&mut self, key: &[u8]) {
        if self.block.is_full() {
            self.flush_block();
        }
        self.block.add_delete(key);
        self.last_key_in_block.clear();
        self.last_key_in_block.extend_from_slice(key);
    }

    pub fn finish(mut self) -> std::io::Result<(TableId, BlockHandle)> {
        if !self.block.is_empty() || !self.last_key_in_block.is_empty() {
            self.flush_block();
        }
        let index_bytes = std::mem::take(&mut self.index).encode();
        let index_offset = self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&index_bytes)?;
        let index_len = index_bytes.len() as u32;
        let mut footer = Vec::with_capacity(FOOTER_SIZE);
        footer.extend_from_slice(&index_offset.to_le_bytes());
        footer.extend_from_slice(&index_len.to_le_bytes());
        footer.extend_from_slice(&SSTABLE_VERSION.to_le_bytes());
        footer.extend_from_slice(&SSTABLE_MAGIC.to_le_bytes());
        self.file.write_all(&footer)?;
        self.file.flush()?;
        self.file.sync_all()?;
        Ok((
            0 as TableId,
            BlockHandle {
                offset: index_offset,
                length: index_len,
            },
        ))
    }
}

impl SsTableBuilder {
    fn flush_block(&mut self) {
        let start = self.file.seek(SeekFrom::End(0)).expect("seek");
        let data = std::mem::replace(&mut self.block, DataBlock::new(self.block_size)).encode();
        self.file.write_all(&data).expect("write block");
        let handle = BlockHandle {
            offset: start,
            length: data.len() as u32,
        };
        self.index.add(&self.last_key_in_block, handle);
    }
}
