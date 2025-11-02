use super::TableId;
use crate::storage::sstable::{index::Index, FOOTER_SIZE, SSTABLE_MAGIC, SSTABLE_VERSION};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

pub struct SsTableReader {
    file: File,
    index: Index,
}

impl SsTableReader {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let mut file = File::open(path)?;
        let len = file.metadata()?.len();
        if len < FOOTER_SIZE as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short sstable",
            ));
        }
        file.seek(SeekFrom::Start(len - FOOTER_SIZE as u64))?;
        let mut footer = [0u8; 24];
        file.read_exact(&mut footer)?;
        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_len = u32::from_le_bytes(footer[8..12].try_into().unwrap()) as usize;
        let version = u32::from_le_bytes(footer[12..16].try_into().unwrap());
        let magic = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        if magic != SSTABLE_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bad magic",
            ));
        }
        if version != SSTABLE_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bad version",
            ));
        }
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_buf = vec![0u8; index_len];
        file.read_exact(&mut index_buf)?;
        let index = Index::decode(&index_buf[..])?;
        Ok(Self { file, index })
    }

    pub fn table_id(&self) -> TableId {
        0
    }

    pub fn get(&self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let handle = match self.index.find_block(key) {
            Some(h) => h,
            None => return Ok(None),
        };
        let mut buf = vec![0u8; handle.length as usize];
        let mut f = &self.file;
        f.seek(SeekFrom::Start(handle.offset))?;
        f.read_exact(&mut buf)?;
        if buf.len() < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short block",
            ));
        }
        let crc_stored = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        let payload = &buf[..buf.len() - 4];
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(payload);
        let crc_calc = hasher.finalize();
        if crc_calc != crc_stored {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "block crc",
            ));
        }
        let mut p = 0usize;
        let mut found: Option<(u8, Vec<u8>)> = None;
        while p < payload.len() {
            if p + 1 + 4 + 4 > payload.len() {
                break;
            }
            let op = payload[p];
            p += 1;
            let klen = u32::from_le_bytes(payload[p..p + 4].try_into().unwrap()) as usize;
            p += 4;
            let vlen = u32::from_le_bytes(payload[p..p + 4].try_into().unwrap()) as usize;
            p += 4;
            if p + klen > payload.len() {
                break;
            }
            let k = &payload[p..p + klen];
            p += klen;
            if op == 0 {
                if p + vlen > payload.len() {
                    break;
                }
                let v = &payload[p..p + vlen];
                p += vlen;
                if k == key {
                    found = Some((0, v.to_vec()));
                }
            } else if k == key {
                found = Some((1, Vec::new()));
            }
        }
        match found {
            Some((0, v)) => Ok(Some(v)),
            Some((1, _)) => Ok(None),
            _ => Ok(None),
        }
    }
}
