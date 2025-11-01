pub trait CRDT: Sized {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Self;
    fn merge(&mut self, other: &Self);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GSet {
    elems: Vec<Vec<u8>>, 
}

impl GSet {
    pub fn new() -> Self { Self { elems: Vec::new() } }

    pub fn contains(&self, k: &[u8]) -> bool {
        self.elems.binary_search_by(|e| e.as_slice().cmp(k)).is_ok()
    }

    pub fn insert(&mut self, k: Vec<u8>) {
        match self.elems.binary_search(&k) {
            Ok(_) => {}
            Err(i) => self.elems.insert(i, k),
        }
    }

    pub fn len(&self) -> usize { self.elems.len() }

    pub fn iter(&self) -> impl Iterator<Item=&Vec<u8>> { self.elems.iter() }

    pub fn elements(&self) -> Vec<Vec<u8>> {
        self.elems.clone()
    }
}

impl CRDT for GSet {
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(&(self.elems.len() as u32).to_be_bytes());
        for e in &self.elems {
            out.extend(&(e.len() as u32).to_be_bytes());
            out.extend(e);
        }
        out
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        use std::convert::TryInto;
        let mut i = 0;
        if bytes.len() < 4 { return GSet::new(); }
        let cnt = u32::from_be_bytes(bytes[i..i+4].try_into().unwrap()) as usize; i += 4;
        let mut elems = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            if i + 4 > bytes.len() { break; }
            let l = u32::from_be_bytes(bytes[i..i+4].try_into().unwrap()) as usize; i += 4;
            if i + l > bytes.len() { break; }
            elems.push(bytes[i..i+l].to_vec());
            i += l;
        }
        elems.sort();
        elems.dedup();
        GSet { elems }
    }

    fn merge(&mut self, other: &Self) {
        let mut out = Vec::with_capacity(self.elems.len() + other.elems.len());
        let mut a = &self.elems[..];
        let mut b = &other.elems[..];
        let mut ia = 0usize;
        let mut ib = 0usize;
        while ia < a.len() && ib < b.len() {
            let av = &a[ia];
            let bv = &b[ib];
            match av.cmp(bv) {
                std::cmp::Ordering::Less => { out.push(av.clone()); ia += 1; }
                std::cmp::Ordering::Greater => { out.push(bv.clone()); ib += 1; }
                std::cmp::Ordering::Equal => { out.push(av.clone()); ia += 1; ib += 1; }
            }
        }
        while ia < a.len() { out.push(a[ia].clone()); ia += 1; }
        while ib < b.len() { out.push(b[ib].clone()); ib += 1; }
        self.elems = out;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(v: &str) -> Vec<u8> { v.as_bytes().to_vec() }

    // Basic union test
    #[test]
    fn merge_union_basic() {
        let mut g1 = GSet::new();
        g1.insert(b("a"));
        g1.insert(b("b"));

        let mut g2 = GSet::new();
        g2.insert(b("b"));
        g2.insert(b("c"));

        g1.merge(&g2);

        let expected = vec![b("a"), b("b"), b("c")];
        assert_eq!(g1.elements(), expected);
    }

    //Serialize, deserialize, merge
    #[test]
    fn merge_from_serialized_blobs() {
        let mut local = GSet::new();
        local.insert(b("alpha"));
        local.insert(b("beta"));

        let mut remote = GSet::new();
        remote.insert(b("beta"));
        remote.insert(b("gamma"));

        println!("Local before merge: {:?}",local.elements().iter().map(|v| String::from_utf8_lossy(v)).collect::<Vec<_>>());
        println!("Remote before merge: {:?}", remote.elements().iter().map(|v| String::from_utf8_lossy(v)).collect::<Vec<_>>());

        let remote_bytes = remote.to_bytes();
        println!("Remote serialized bytes (hex): {}", hex::encode(&remote_bytes));

        let remote_deser = GSet::from_bytes(&remote_bytes);
        println!("Remote deserialized: {:?}", remote_deser.elements().iter().map(|v| String::from_utf8_lossy(v)).collect::<Vec<_>>());

        local.merge(&remote_deser);
        println!("Local after merge: {:?}", local.elements().iter().map(|v| String::from_utf8_lossy(v)).collect::<Vec<_>>());

        let expected = vec![b("alpha"), b("beta"), b("gamma")];
        assert_eq!(local.elements(), expected);
    }

    //malformed input: unsorted elements and duplicates
    #[test]
    fn merge_handles_unsorted_and_duplicates_in_bytes() {
        let mut blob = Vec::new();
        // count = 4
        blob.extend(&(4u32.to_be_bytes()));
        // "z"
        blob.extend(&(1u32.to_be_bytes())); blob.extend(b"z");
        // "a"
        blob.extend(&(1u32.to_be_bytes())); blob.extend(b"a");
        // "a" duplicate
        blob.extend(&(1u32.to_be_bytes())); blob.extend(b"a");
        // "m"
        blob.extend(&(1u32.to_be_bytes())); blob.extend(b"m");

        let mut local = GSet::new();
        local.insert(b("b"));
        local.insert(b("m"));

        let remote = GSet::from_bytes(&blob);
        assert_eq!(remote.elements(), vec![b("a"), b("m"), b("z")]);
        local.merge(&remote);
        assert_eq!(local.elements(), vec![b("a"), b("b"), b("m"), b("z")]);
    }
}