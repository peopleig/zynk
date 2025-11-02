pub trait CRDT: Sized {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Self;
    fn merge(&mut self, other: &Self);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GSet {
    elems: Vec<Vec<u8>>,
}

impl Default for GSet {
    fn default() -> Self {
        Self::new()
    }
}

impl GSet {
    pub fn new() -> Self {
        Self { elems: Vec::new() }
    }

    pub fn contains(&self, k: &[u8]) -> bool {
        self.elems.binary_search_by(|e| e.as_slice().cmp(k)).is_ok()
    }

    pub fn insert(&mut self, k: Vec<u8>) {
        match self.elems.binary_search(&k) {
            Ok(_) => {}
            Err(i) => self.elems.insert(i, k),
        }
    }

    pub fn len(&self) -> usize {
        self.elems.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.elems.iter()
    }

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
        if bytes.len() < 4 {
            return GSet::new();
        }
        let cnt = u32::from_be_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        let mut elems = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            if i + 4 > bytes.len() {
                break;
            }
            let l = u32::from_be_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
            i += 4;
            if i + l > bytes.len() {
                break;
            }
            elems.push(bytes[i..i + l].to_vec());
            i += l;
        }
        elems.sort();
        elems.dedup();
        GSet { elems }
    }

    fn merge(&mut self, other: &Self) {
        let mut out = Vec::with_capacity(self.elems.len() + other.elems.len());
        let a = &self.elems[..];
        let b = &other.elems[..];
        let mut ia = 0usize;
        let mut ib = 0usize;
        while ia < a.len() && ib < b.len() {
            let av = &a[ia];
            let bv = &b[ib];
            match av.cmp(bv) {
                std::cmp::Ordering::Less => {
                    out.push(av.clone());
                    ia += 1;
                }
                std::cmp::Ordering::Greater => {
                    out.push(bv.clone());
                    ib += 1;
                }
                std::cmp::Ordering::Equal => {
                    out.push(av.clone());
                    ia += 1;
                    ib += 1;
                }
            }
        }
        while ia < a.len() {
            out.push(a[ia].clone());
            ia += 1;
        }
        while ib < b.len() {
            out.push(b[ib].clone());
            ib += 1;
        }
        self.elems = out;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(v: &str) -> Vec<u8> {
        v.as_bytes().to_vec()
    }

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

        println!(
            "Local before merge: {:?}",
            local
                .elements()
                .iter()
                .map(|v| String::from_utf8_lossy(v))
                .collect::<Vec<_>>()
        );
        println!(
            "Remote before merge: {:?}",
            remote
                .elements()
                .iter()
                .map(|v| String::from_utf8_lossy(v))
                .collect::<Vec<_>>()
        );

        let remote_bytes = remote.to_bytes();
        println!(
            "Remote serialized bytes (hex): {}",
            hex::encode(&remote_bytes)
        );

        let remote_deser = GSet::from_bytes(&remote_bytes);
        println!(
            "Remote deserialized: {:?}",
            remote_deser
                .elements()
                .iter()
                .map(|v| String::from_utf8_lossy(v))
                .collect::<Vec<_>>()
        );

        local.merge(&remote_deser);
        println!(
            "Local after merge: {:?}",
            local
                .elements()
                .iter()
                .map(|v| String::from_utf8_lossy(v))
                .collect::<Vec<_>>()
        );

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
        blob.extend(&(1u32.to_be_bytes()));
        blob.extend(b"z");
        // "a"
        blob.extend(&(1u32.to_be_bytes()));
        blob.extend(b"a");
        // "a" duplicate
        blob.extend(&(1u32.to_be_bytes()));
        blob.extend(b"a");
        // "m"
        blob.extend(&(1u32.to_be_bytes()));
        blob.extend(b"m");

        let mut local = GSet::new();
        local.insert(b("b"));
        local.insert(b("m"));

        let remote = GSet::from_bytes(&blob);
        assert_eq!(remote.elements(), vec![b("a"), b("m"), b("z")]);
        local.merge(&remote);
        assert_eq!(local.elements(), vec![b("a"), b("b"), b("m"), b("z")]);
    }
}

use std::collections::BTreeMap;

/// Unique identifier for an RGA element: (actor, counter)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ElementId {
    pub actor: u64,
    pub counter: u64,
}

impl ElementId {
    pub fn new(actor: u64, counter: u64) -> Self {
        Self { actor, counter }
    }

    fn to_bytes(&self, out: &mut Vec<u8>) {
        out.extend(&self.actor.to_be_bytes());
        out.extend(&self.counter.to_be_bytes());
    }

    fn from_bytes(bs: &[u8]) -> Option<(Self, usize)> {
        if bs.len() < 16 {
            return None;
        }
        let actor = u64::from_be_bytes(bs[0..8].try_into().unwrap());
        let counter = u64::from_be_bytes(bs[8..16].try_into().unwrap());
        Some((ElementId { actor, counter }, 16))
    }
}

#[derive(Debug, Clone)]
pub struct Element {
    pub id: ElementId,
    pub prev: Option<ElementId>,
    pub value: Vec<u8>,
    pub deleted: bool,
}

impl Element {
    fn to_bytes(&self, out: &mut Vec<u8>) {
        self.id.to_bytes(out);
        match &self.prev {
            Some(pid) => {
                out.push(1);
                pid.to_bytes(out);
            }
            None => {
                out.push(0);
            }
        }
        out.extend(&(self.value.len() as u32).to_be_bytes());
        out.extend(&self.value);
        out.push(if self.deleted { 1 } else { 0 });
    }

    fn from_bytes(bs: &[u8]) -> Option<(Self, usize)> {
        let mut i = 0usize;
        let (id, n) = ElementId::from_bytes(&bs[i..])?;
        i += n;
        if i >= bs.len() {
            return None;
        }
        let has_prev = bs[i] == 1;
        i += 1;
        let prev = if has_prev {
            let (pid, m) = ElementId::from_bytes(&bs[i..])?;
            i += m;
            Some(pid)
        } else {
            None
        };
        if i + 4 > bs.len() {
            return None;
        }
        let vlen = u32::from_be_bytes(bs[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        if i + vlen > bs.len() {
            return None;
        }
        let value = bs[i..i + vlen].to_vec();
        i += vlen;
        if i >= bs.len() {
            return None;
        }
        let deleted = bs[i] != 0;
        i += 1;
        Some((
            Element {
                id,
                prev,
                value,
                deleted,
            },
            i,
        ))
    }
}

/// State-based RGA: map ElementId -> Element
#[derive(Debug, Clone)]
pub struct Rga {
    pub elems: BTreeMap<ElementId, Element>,
}

impl Default for Rga {
    fn default() -> Self {
        Self::new()
    }
}

impl Rga {
    pub fn new() -> Self {
        Self {
            elems: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, id: ElementId, prev: Option<ElementId>, value: Vec<u8>) {
        self.elems.entry(id).or_insert(Element {
            id,
            prev,
            value,
            deleted: false,
        });
    }

    pub fn delete(&mut self, id: ElementId) {
        if let Some(e) = self.elems.get_mut(&id) {
            e.deleted = true;
        } else {
            self.elems.insert(
                id,
                Element {
                    id,
                    prev: None,
                    value: Vec::new(),
                    deleted: true,
                },
            );
        }
    }

    pub fn visible_sequence(&self) -> Vec<Vec<u8>> {
        let mut children: BTreeMap<Option<ElementId>, Vec<ElementId>> = BTreeMap::new();
        for (id, elem) in &self.elems {
            children.entry(elem.prev).or_default().push(*id);
        }

        for vec in children.values_mut() {
            vec.sort();
        }

        fn visit(
            curr: Option<ElementId>,
            children: &BTreeMap<Option<ElementId>, Vec<ElementId>>,
            elems: &BTreeMap<ElementId, Element>,
            out: &mut Vec<Vec<u8>>,
        ) {
            if let Some(kids) = children.get(&curr) {
                for id in kids {
                    if let Some(elem) = elems.get(id) {
                        if !elem.deleted {
                            out.push(elem.value.clone());
                        }
                        visit(Some(*id), children, elems, out);
                    }
                }
            }
        }

        let mut out = Vec::new();
        visit(None, &children, &self.elems, &mut out);
        out
    }

    pub fn merge(&mut self, other: &Self) {
        for (id, other_elem) in &other.elems {
            match self.elems.get_mut(id) {
                Some(local) => {
                    // merge tombstone and keep existing value if present and if local lacks value but other has one, take it.
                    local.deleted = local.deleted || other_elem.deleted;
                    if local.value.is_empty() && !other_elem.value.is_empty() {
                        local.value = other_elem.value.clone();
                    }
                    if local.prev.is_none() && other_elem.prev.is_some() {
                        local.prev = other_elem.prev;
                    }
                }
                None => {
                    self.elems.insert(*id, other_elem.clone());
                }
            }
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(&(self.elems.len() as u32).to_be_bytes());
        for elem in self.elems.values() {
            elem.to_bytes(&mut out);
        }
        out
    }

    pub fn from_bytes(bs: &[u8]) -> Self {
        let mut i = 0usize;
        if bs.len() < 4 {
            return Rga::new();
        }
        let count = u32::from_be_bytes(bs[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        let mut elems = BTreeMap::new();
        for _ in 0..count {
            if i >= bs.len() {
                break;
            }
            if let Some((elem, n)) = Element::from_bytes(&bs[i..]) {
                elems.insert(elem.id, elem);
                i += n;
            } else {
                break;
            }
        }
        Rga { elems }
    }
}

impl CRDT for Rga {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_bytes()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        Rga::from_bytes(bytes)
    }
    fn merge(&mut self, other: &Self) {
        self.merge(other)
    }
}

#[cfg(test)]
mod rga_tests {
    use super::*;

    fn s(v: &str) -> Vec<u8> {
        v.as_bytes().to_vec()
    }

    fn show_seq(seq: &[Vec<u8>]) -> Vec<String> {
        seq.iter()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .collect()
    }

    #[test]
    fn rga_basic_insert_tail() {
        println!("TEST: rga_basic_insert_tail");
        let mut r = Rga::new();

        let a = ElementId::new(1, 1);
        r.insert(a, None, s("A"));
        println!("After insert A: {:?}", show_seq(&r.visible_sequence()));

        let b = ElementId::new(1, 2);
        r.insert(b, Some(a), s("B"));
        println!(
            "After insert B after A: {:?}",
            show_seq(&r.visible_sequence())
        );

        assert_eq!(
            show_seq(&r.visible_sequence()),
            vec!["A".to_string(), "B".to_string()]
        );
    }

    #[test]
    fn rga_insert_at_head_and_sibling_ordering() {
        println!("TEST: rga_insert_at_head_and_sibling_ordering");
        let mut r = Rga::new();

        // two inserts at head (prev = None) with different ids -> deterministic order by ElementId
        let id1 = ElementId::new(2, 1); // actor 2
        let id2 = ElementId::new(1, 1); // actor 1 (id2 < id1)
        r.insert(id1, None, s("X"));
        r.insert(id2, None, s("Y"));

        println!(
            "Elements inserted at head: {:?}",
            show_seq(&r.visible_sequence())
        );
        // ordering by ElementId (actor then counter) -> id2 (actor1) before id1 (actor2)
        assert_eq!(
            show_seq(&r.visible_sequence()),
            vec!["Y".to_string(), "X".to_string()]
        );
    }

    #[test]
    fn rga_delete_element() {
        println!("TEST: rga_delete_element");
        let mut r = Rga::new();
        let a = ElementId::new(1, 1);
        let b = ElementId::new(1, 2);
        let c = ElementId::new(1, 3);

        r.insert(a, None, s("one"));
        r.insert(b, Some(a), s("two"));
        r.insert(c, Some(b), s("three"));
        println!("Before delete: {:?}", show_seq(&r.visible_sequence()));

        r.delete(b);
        println!(
            "After delete of 'two' (tombstoned): {:?}",
            show_seq(&r.visible_sequence())
        );

        assert_eq!(
            show_seq(&r.visible_sequence()),
            vec!["one".to_string(), "three".to_string()]
        );
    }

    #[test]
    fn rga_merge_concurrent_inserts() {
        println!("TEST: rga_merge_concurrent_inserts");
        // replica 1
        let mut r1 = Rga::new();
        let a1 = ElementId::new(1, 1);
        r1.insert(a1, None, s("A"));

        // replica 2
        let mut r2 = Rga::new();
        let b2 = ElementId::new(2, 1);
        r2.insert(b2, None, s("B"));

        println!("r1 before merge: {:?}", show_seq(&r1.visible_sequence()));
        println!("r2 before merge: {:?}", show_seq(&r2.visible_sequence()));

        // merge r2 into r1
        r1.merge(&r2);
        println!(
            "r1 after merge r2->r1: {:?}",
            show_seq(&r1.visible_sequence())
        );

        // merge is commutative; resulting visible sequence deterministic by ElementId ordering:
        assert_eq!(
            show_seq(&r1.visible_sequence()),
            vec!["A".to_string(), "B".to_string()]
        );
    }

    #[test]
    fn rga_merge_delete_tombstone() {
        println!("TEST: rga_merge_delete_tombstone");
        // replica 1 has A,B
        let mut r1 = Rga::new();
        let a1 = ElementId::new(1, 1);
        let b1 = ElementId::new(1, 2);
        r1.insert(a1, None, s("A"));
        r1.insert(b1, Some(a1), s("B"));

        // replica 2 only carries a tombstone for B (delete)
        let mut r2 = Rga::new();
        r2.delete(b1);

        println!("r1 before merge: {:?}", show_seq(&r1.visible_sequence()));
        println!(
            "r2 (tombstone) before merge: {:?}",
            show_seq(&r2.visible_sequence())
        );

        r1.merge(&r2);
        println!(
            "r1 after merging tombstone: {:?}",
            show_seq(&r1.visible_sequence())
        );

        assert_eq!(show_seq(&r1.visible_sequence()), vec!["A".to_string()]);
    }

    #[test]
    fn rga_serialize_roundtrip() {
        println!("TEST: rga_serialize_roundtrip");
        let mut r = Rga::new();
        r.insert(ElementId::new(3, 1), None, s("alpha"));
        r.insert(ElementId::new(3, 2), Some(ElementId::new(3, 1)), s("beta"));
        println!("Original visible: {:?}", show_seq(&r.visible_sequence()));

        let bytes = r.to_bytes();
        println!(
            "Serialized bytes (len={}): {:02x?}",
            bytes.len(),
            &bytes[..std::cmp::min(bytes.len(), 64)]
        );
        let r2 = Rga::from_bytes(&bytes);
        println!(
            "Deserialized visible: {:?}",
            show_seq(&r2.visible_sequence())
        );

        assert_eq!(
            show_seq(&r.visible_sequence()),
            show_seq(&r2.visible_sequence())
        );
    }

    #[test]
    fn rga_complex_merge_with_prev_links() {
        println!("TEST: rga_complex_merge_with_prev_links");
        // actor1 inserts A (1,1)
        let mut r1 = Rga::new();
        let a = ElementId::new(1, 1);
        r1.insert(a, None, s("A"));

        // actor2 inserts B after A concurrently
        let mut r2 = Rga::new();
        let b = ElementId::new(2, 1);
        r2.insert(b, Some(a), s("B"));

        // actor1 inserts C after A concurrently
        r1.insert(ElementId::new(1, 2), Some(a), s("C"));

        println!("r1 before merge: {:?}", show_seq(&r1.visible_sequence())); // A, C
        println!("r2 before merge: {:?}", show_seq(&r2.visible_sequence())); // B

        // merge r2 -> r1
        r1.merge(&r2);
        println!("r1 after merge: {:?}", show_seq(&r1.visible_sequence()));

        // children of A are ids (1,2) and (2,1). Ordering: ElementId ord -> (1,2) then (2,1)
        assert_eq!(
            show_seq(&r1.visible_sequence()),
            vec!["A".to_string(), "C".to_string(), "B".to_string()]
        );
    }
}
