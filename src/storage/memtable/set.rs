use super::table::{Entry, MemTable};

pub struct MemTableSet {
    active: MemTable,
    immutables: Vec<MemTable>,
    max_bytes: usize,
}

impl MemTableSet {
    pub fn with_capacity(max_bytes: usize) -> Self {
        Self {
            active: MemTable::new(max_bytes),
            immutables: Vec::new(),
            max_bytes,
        }
    }

    pub fn active_bytes(&self) -> usize {
        self.active.bytes_used()
    }

    pub fn immutables_len(&self) -> usize {
        self.immutables.len()
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Option<MemTable> {
        self.active.put(key, value);
        if self.active.over_threshold() {
            return self.rotate();
        }
        None
    }

    pub fn delete(&mut self, key: &[u8]) -> Option<MemTable> {
        self.active.delete(key);
        if self.active.over_threshold() {
            return self.rotate();
        }
        None
    }

    pub fn rotate(&mut self) -> Option<MemTable> {
        if self.active.is_empty() {
            return None;
        }
        let frozen = std::mem::replace(&mut self.active, MemTable::new(self.max_bytes));
        self.immutables.push(frozen);
        self.immutables.last().cloned()
    }

    pub fn pop_immutable(&mut self) -> Option<MemTable> {
        self.immutables.pop()
    }

    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        if let Some(e) = self.active.get(key) {
            return Some(e);
        }
        for mt in self.immutables.iter().rev() {
            if let Some(e) = mt.get(key) {
                return Some(e);
            }
        }
        None
    }
}
