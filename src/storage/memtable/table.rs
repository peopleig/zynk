use std::collections::BTreeMap;

#[derive(Clone)]
pub enum Entry {
    Put(Vec<u8>),
    Delete,
}

#[derive(Clone)]
pub struct MemTable {
    map: BTreeMap<Vec<u8>, Entry>,
    bytes_used: usize,
    max_bytes: usize,
}

impl MemTable {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            map: BTreeMap::new(),
            bytes_used: 0,
            max_bytes,
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn bytes_used(&self) -> usize {
        self.bytes_used
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.adjust_remove(key);
        self.bytes_used += 1 + 4 + 4 + key.len() + value.len();
        self.map.insert(key.to_vec(), Entry::Put(value.to_vec()));
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.adjust_remove(key);
        self.bytes_used += 1 + 4 + 4 + key.len();
        self.map.insert(key.to_vec(), Entry::Delete);
    }

    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.map.get(key)
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.map.contains_key(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Entry)> {
        self.map.iter()
    }

    pub fn smallest_key(&self) -> Option<&[u8]> {
        self.map.keys().next().map(|k| k.as_slice())
    }

    pub fn largest_key(&self) -> Option<&[u8]> {
        self.map.keys().next_back().map(|k| k.as_slice())
    }

    pub fn over_threshold(&self) -> bool {
        self.bytes_used >= self.max_bytes
    }

    fn adjust_remove(&mut self, key: &[u8]) {
        if let Some(prev) = self.map.get(key) {
            match prev {
                Entry::Put(v) => {
                    self.bytes_used = self
                        .bytes_used
                        .saturating_sub(1 + 4 + 4 + key.len() + v.len());
                }
                Entry::Delete => {
                    self.bytes_used = self.bytes_used.saturating_sub(1 + 4 + 4 + key.len());
                }
            }
        }
    }
}
