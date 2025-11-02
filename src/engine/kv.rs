use crate::engine::crdt::{ElementId, Rga};
use crate::storage::manifest::{fsync_dir, open_manifest_append, read_current_or_init, Manifest};
use crate::storage::memtable::{flush_memtable_to_sstable, Entry, MemTable, MemTableSet};
use crate::storage::sstable::{reader::SsTableReader, TableId};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct LsmEngine {
    data_dir: PathBuf,
    memtables: MemTableSet,
    sstables: Vec<(TableId, PathBuf, SsTableReader)>,
    block_bytes: usize,
    pub actor_id: u64,
    local_counter: AtomicU64,
    next_table_id: TableId,
    manifest: Manifest,
}

impl LsmEngine {
    pub fn new<P: AsRef<Path>>(
        data_dir: P,
        memtable_max_bytes: usize,
        block_bytes: usize,
    ) -> std::io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let sst_dir = data_dir.join("sst");
        fs::create_dir_all(&sst_dir)?;
        let memtables = MemTableSet::with_capacity(memtable_max_bytes);
        let manifest = Manifest::new(data_dir.join("MANIFEST-000001"))?;
        Ok(Self {
            data_dir,
            memtables,
            sstables: Vec::new(),
            block_bytes,
            actor_id: 0,
            local_counter: AtomicU64::new(0),
            next_table_id: 1,
            manifest,
        })
    }

    pub fn new_with_manifest<P: AsRef<Path>>(
        data_dir: P,
        memtable_max_bytes: usize,
        block_bytes: usize,
    ) -> std::io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let sst_dir = data_dir.join("sst");
        fs::create_dir_all(&sst_dir)?;

        let memtables = MemTableSet::with_capacity(memtable_max_bytes);
        let name = read_current_or_init(&data_dir, "MANIFEST-000001")?;
        let mut manifest = open_manifest_append(&data_dir, &name)?;
        let active_tables_ids = manifest.replay_manifest()?;

        let mut sstables = Vec::new();
        for id in active_tables_ids.clone() {
            let path = data_dir.join("sst").join(format!("{id:06}.sst"));
            if let Ok(reader) = SsTableReader::open(&path) {
                sstables.push((id, path, reader));
            }
        }

        let next_table_id = active_tables_ids.iter().copied().max().unwrap_or(0) + 1;

        Ok(Self {
            data_dir,
            memtables,
            sstables,
            block_bytes,
            actor_id: 0,
            local_counter: AtomicU64::new(0),
            next_table_id,
            manifest,
        })
    }

    pub fn new_with_manifest_and_actor(
        data_dir: &std::path::Path,
        memtable_size: usize,
        block_size: usize,
        actor_id: u64,
    ) -> std::io::Result<Self> {
        let mut eng = Self::new_with_manifest(data_dir, memtable_size, block_size)?;
        eng.actor_id = actor_id;
        eng.local_counter = AtomicU64::new(1);
        Ok(eng)
    }

    /// Generate a fresh ElementId for local inserts.
    pub fn next_element_id(&self) -> ElementId {
        let ctr = self.local_counter.fetch_add(1, Ordering::SeqCst);
        ElementId::new(self.actor_id, ctr)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        if let Some(frozen) = self.memtables.put(key, value) {
            self.flush_immutable(frozen)?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> std::io::Result<()> {
        if let Some(frozen) = self.memtables.delete(key) {
            self.flush_immutable(frozen)?;
        }
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        if let Some(entry) = self.memtables.get(key) {
            return Ok(match entry {
                Entry::Put(v) => Some(v.clone()),
                Entry::Delete => None,
            });
        }
        for (_, _path, reader) in self.sstables.iter().rev() {
            if let Some(v) = reader.get(key)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        if let Some(frozen) = self.memtables.rotate() {
            self.flush_immutable(frozen)?;
        }
        Ok(())
    }

    pub fn gset_add(&mut self, key: Vec<u8>, elem: Vec<u8>) -> std::io::Result<()> {
        use crate::engine::crdt::{GSet, CRDT};

        if let Some(entry) = self.memtables.get(&key) {
            if let crate::storage::memtable::Entry::Put(existing_bytes) = entry {
                let mut gs = GSet::from_bytes(existing_bytes);
                gs.insert(elem);
                let new_bytes = gs.to_bytes();
                if let Some(frozen) = self.memtables.put(&key, &new_bytes) {
                    self.flush_immutable(frozen)?;
                }
                return Ok(());
            }
        }

        for (_, _path, reader) in self.sstables.iter().rev() {
            if let Some(bytes) = reader.get(&key)? {
                let mut gs = GSet::from_bytes(&bytes);
                gs.insert(elem);
                let new_bytes = gs.to_bytes();
                if let Some(frozen) = self.memtables.put(&key, &new_bytes) {
                    self.flush_immutable(frozen)?;
                }
                return Ok(());
            }
        }

        let mut gs = GSet::new();
        gs.insert(elem);
        let new_bytes = gs.to_bytes();
        if let Some(frozen) = self.memtables.put(&key, &new_bytes) {
            self.flush_immutable(frozen)?;
        }
        Ok(())
    }

    pub fn gset_get(&self, key: &[u8]) -> std::io::Result<Vec<Vec<u8>>> {
        use crate::engine::crdt::{GSet, CRDT};
        use crate::storage::memtable::Entry;

        let mut result = GSet::new();

        if let Some(entry) = self.memtables.get(key) {
            if let Entry::Put(bytes) = entry {
                let gs = GSet::from_bytes(bytes);
                result.merge(&gs);
            }
        }

        for (_, _path, reader) in self.sstables.iter().rev() {
            if let Some(bytes) = reader.get(key)? {
                let gs = GSet::from_bytes(&bytes);
                result.merge(&gs);
            }
        }

        Ok(result.elements())
    }

    pub fn rga_insert_after(
        &mut self,
        key: &[u8],
        prev: Option<ElementId>,
        value: Vec<u8>,
        actor_id: u64,
        counter: u64,
    ) -> std::io::Result<()> {
        let mut rga = match self.get(key)? {
            Some(bs) => Rga::from_bytes(&bs),
            None => Rga::new(),
        };

        let id = ElementId::new(actor_id, counter);
        rga.insert(id, prev, value.clone());

        // println!(
        //     "RGA INSERT -> key={}, id=({}:{}) prev={:?} value='{}'",
        //     String::from_utf8_lossy(key),
        //     actor_id,
        //     counter,
        //     prev,
        //     String::from_utf8_lossy(&value)
        // );

        // println!(
        //     "Current sequence for '{}': {:?}",
        //     String::from_utf8_lossy(key),
        //     rga.visible_sequence()
        //         .iter()
        //         .map(|v| String::from_utf8_lossy(v).to_string())
        //         .collect::<Vec<_>>()
        // );

        let bytes = rga.to_bytes();
        self.put(key, &bytes)
    }

    pub fn rga_delete(&mut self, key: &[u8], id: ElementId) -> std::io::Result<()> {
        let mut rga = match self.get(key)? {
            Some(bs) => Rga::from_bytes(&bs),
            None => return Ok(()), // kuch nai hein delete karne ko
        };
        rga.delete(id);
        self.put(key, &rga.to_bytes())
    }

    pub fn rga_get_visible(&self, key: &[u8]) -> std::io::Result<Vec<Vec<u8>>> {
        match self.get(key)? {
            Some(bs) => {
                let rga = Rga::from_bytes(&bs);
                Ok(rga.visible_sequence())
            }
            None => Ok(vec![]),
        }
    }

    fn flush_immutable(&mut self, frozen: MemTable) -> Result<(), std::io::Error> {
        let id = self.alloc_table_id();
        let tmp = self.sst_tmp_path(id);
        let final_path = self.sst_final_path(id);

        let _ = fs::create_dir_all(final_path.parent().unwrap());
        let _res = flush_memtable_to_sstable(frozen, &tmp, self.block_bytes)?;

        fs::rename(&tmp, &final_path)?;
        fsync_dir(&final_path)?;
        self.manifest.record_add_table(id)?;

        let reader = SsTableReader::open(&final_path)?;
        self.sstables.push((id, final_path, reader));
        Ok(())
    }

    fn sst_tmp_path(&self, id: TableId) -> PathBuf {
        self.data_dir.join("sst").join(format!("{id:06}.sst.tmp"))
    }

    fn sst_final_path(&self, id: TableId) -> PathBuf {
        self.data_dir.join("sst").join(format!("{id:06}.sst"))
    }

    fn alloc_table_id(&mut self) -> TableId {
        let id = self.next_table_id;
        self.next_table_id += 1;
        id
    }
}
