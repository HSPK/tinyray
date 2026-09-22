use super::*;

#[derive(Default)]
pub struct CachedPool {
    pub version: u64,
    pub roster: u64,
    pub methods: Arc<[String]>,
    pub size: Option<u64>,
    pub members: HashMap<u64, Arc<Member>>,
    pub(super) slots: HashMap<u64, BTreeSet<u64>>,
    pub(super) ids: OnceLock<Vec<u64>>,
    pub(super) ready_ids: OnceLock<Vec<u64>>,
    pub(super) snapshots: Mutex<[Option<SerializedMembers>; 2]>,
    pub(super) native: Mutex<[Option<Arc<FrozenMembers>>; 2]>,
    pub(super) digest: Mutex<Option<(Vec<String>, u64)>>,
    pub(super) filter_index: Mutex<FilterIndexCache>,
}

pub(super) struct SerializedMembers {
    bytes: Vec<u8>,
    roster: u64,
}

#[derive(Debug)]
pub struct FrozenMembers {
    members: Box<[Arc<Member>]>,
    ids: HashMap<u64, usize>,
    slots: HashMap<u64, Box<[usize]>>,
    roster: u64,
    states: OnceLock<Arc<[u8]>>,
}

impl FrozenMembers {
    fn new(mut members: Vec<Arc<Member>>) -> Self {
        members.sort_unstable_by_key(|member| member.id);
        let mut ids = HashMap::with_capacity(members.len());
        let mut slots: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut roster = 0;
        for (index, member) in members.iter().enumerate() {
            ids.insert(member.id, index);
            if let Some(slot) = member.slot {
                slots.entry(slot).or_default().push(index);
            }
            roster ^= member.roster_hash();
        }
        Self {
            members: members.into_boxed_slice(),
            ids,
            slots: slots
                .into_iter()
                .map(|(slot, indices)| (slot, indices.into_boxed_slice()))
                .collect(),
            roster,
            states: OnceLock::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn roster(&self) -> u64 {
        self.roster
    }

    pub fn members(&self) -> &[Arc<Member>] {
        &self.members
    }

    pub fn slot_arc(&self, slot: u64) -> Option<&Arc<Member>> {
        let index = *self.slots.get(&slot)?.first()?;
        Some(&self.members[index])
    }

    pub fn slot(&self, slot: u64) -> Option<&Member> {
        self.slot_arc(slot).map(Arc::as_ref)
    }

    pub fn get_arc(&self, pool: &str, identity: &str) -> Option<&Arc<Member>> {
        let (named_pool, key, incarnation) = identity_parts(identity)?;
        if named_pool != pool {
            return None;
        }
        if let Some(indices) = self.slots.get(&key) {
            if let Some(member) = indices.iter().find_map(|index| {
                let member = &self.members[*index];
                (member.incarnation == incarnation).then_some(member)
            }) {
                return Some(member);
            }
        }
        let member = &self.members[*self.ids.get(&key)?];
        (member.slot.is_none() && member.incarnation == incarnation).then_some(member)
    }

    pub fn get(&self, pool: &str, identity: &str) -> Option<&Member> {
        self.get_arc(pool, identity).map(Arc::as_ref)
    }

    pub fn serialized_states(&self) -> Arc<[u8]> {
        if let Some(states) = self.states.get() {
            return states.clone();
        }
        let states: Vec<&serde_json::Value> =
            self.members.iter().map(|member| &member.state).collect();
        let encoded =
            rmp_serde::to_vec_named(&states).expect("member states always encode as MessagePack");
        let encoded: Arc<[u8]> = Arc::from(encoded);
        if encoded.len() <= SNAPSHOT_BYTES {
            let _ = self.states.set(encoded.clone());
        }
        encoded
    }
}

#[derive(Clone, Debug)]
pub struct FrozenPool {
    pub members: Arc<FrozenMembers>,
    pub roster: u64,
    pub version: u64,
    pub size: Option<u64>,
    pub methods: Arc<[String]>,
}

fn identity_parts(identity: &str) -> Option<(&str, u64, u64)> {
    let (head, incarnation) = identity.rsplit_once('#')?;
    let (pool, key) = head.rsplit_once('/')?;
    Some((pool, key.parse().ok()?, incarnation.parse().ok()?))
}

pub(super) fn identity_matches(pool: &str, member: &Member, identity: &str) -> bool {
    identity_parts(identity).is_some_and(|(named_pool, key, incarnation)| {
        named_pool == pool
            && key == member.slot.unwrap_or(member.id)
            && incarnation == member.incarnation
    })
}

pub(super) fn member_identity(pool: &str, member: &Member) -> String {
    format!(
        "{pool}/{}#{}",
        member.slot.unwrap_or(member.id),
        member.incarnation
    )
}

// At most two serialized buffers and two Arc-backed views per pool, never an
// entry per filter or revision.
pub(super) const SNAPSHOT_BYTES: usize = 1024 * 1024;
pub(super) const FILTER_INDEX_MAX_ENTRIES: usize = 32;
const FILTER_INDEX_MAX_FIELDS: usize = 8;
pub(super) const FILTER_INDEX_MAX_KEY_BYTES: usize = 4096;
pub(super) const FILTER_INDEX_MAX_IDS_PER_ENTRY: usize = 8192;
const FILTER_INDEX_MAX_BYTES: usize = 1024 * 1024;
const FILTER_INDEX_ENTRY_OVERHEAD: usize = 128;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum IndexedScalar {
    Null,
    Bool(bool),
    String(String),
    I64(i64),
    U64(u64),
    F64(u64),
}

impl IndexedScalar {
    fn from_value(value: &serde_json::Value) -> Option<Self> {
        match value {
            serde_json::Value::Null => Some(Self::Null),
            serde_json::Value::Bool(value) => Some(Self::Bool(*value)),
            serde_json::Value::String(value) => Some(Self::String(value.clone())),
            serde_json::Value::Number(value) if value.is_i64() => {
                Some(Self::I64(value.as_i64().unwrap()))
            }
            serde_json::Value::Number(value) if value.is_u64() => {
                Some(Self::U64(value.as_u64().unwrap()))
            }
            serde_json::Value::Number(value) => {
                let value = value.as_f64()?;
                value.is_finite().then_some(Self::F64(value.to_bits()))
            }
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
        }
    }

    fn estimated_bytes(&self) -> usize {
        match self {
            Self::Null => 1,
            Self::Bool(_) => 2,
            Self::String(value) => value.len() + 1,
            Self::I64(_) | Self::U64(_) | Self::F64(_) => 9,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct IndexedFilterKey {
    require_ready: bool,
    fields: Box<[(String, IndexedScalar)]>,
}

impl IndexedFilterKey {
    fn from_filter(filter: &serde_json::Value, require_ready: bool) -> Option<(Self, usize)> {
        let fields = filter.as_object()?;
        if fields.is_empty() || fields.len() > FILTER_INDEX_MAX_FIELDS {
            return None;
        }
        let mut estimated_bytes = 1;
        let mut indexed = Vec::with_capacity(fields.len());
        for (field, value) in fields {
            let value = IndexedScalar::from_value(value)?;
            estimated_bytes += field.len() + value.estimated_bytes();
            if estimated_bytes > FILTER_INDEX_MAX_KEY_BYTES {
                return None;
            }
            indexed.push((field.clone(), value));
        }
        indexed.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Some((
            Self {
                require_ready,
                fields: indexed.into_boxed_slice(),
            },
            estimated_bytes,
        ))
    }
}

struct FilterIndexEntry {
    ids: Arc<[u64]>,
    estimated_bytes: usize,
    last_used: u64,
}

#[derive(Default)]
pub(super) struct FilterIndexCache {
    entries: HashMap<IndexedFilterKey, FilterIndexEntry>,
    estimated_bytes: usize,
    clock: u64,
    hits: u64,
    builds: u64,
    evictions: u64,
    invalidations: u64,
    fallbacks: u64,
    uncached: u64,
}

impl FilterIndexCache {
    fn next_clock(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        if self.clock == 0 {
            self.clock = 1;
            for entry in self.entries.values_mut() {
                entry.last_used = 0;
            }
        }
        self.clock
    }

    fn get(&mut self, key: &IndexedFilterKey) -> Option<Arc<[u64]>> {
        let now = self.next_clock();
        let entry = self.entries.get_mut(key)?;
        entry.last_used = now;
        self.hits += 1;
        Some(entry.ids.clone())
    }

    fn insert(&mut self, key: IndexedFilterKey, key_bytes: usize, ids: Arc<[u64]>) {
        self.builds += 1;
        let estimated_bytes =
            FILTER_INDEX_ENTRY_OVERHEAD + key_bytes + ids.len() * std::mem::size_of::<u64>();
        if ids.len() > FILTER_INDEX_MAX_IDS_PER_ENTRY || estimated_bytes > FILTER_INDEX_MAX_BYTES {
            self.uncached += 1;
            return;
        }
        if let Some(previous) = self.entries.remove(&key) {
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_sub(previous.estimated_bytes);
        }
        while self.entries.len() >= FILTER_INDEX_MAX_ENTRIES
            || self.estimated_bytes + estimated_bytes > FILTER_INDEX_MAX_BYTES
        {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            let removed = self.entries.remove(&oldest).unwrap();
            self.estimated_bytes = self.estimated_bytes.saturating_sub(removed.estimated_bytes);
            self.evictions += 1;
        }
        if self.estimated_bytes + estimated_bytes > FILTER_INDEX_MAX_BYTES {
            self.uncached += 1;
            return;
        }
        let last_used = self.next_clock();
        self.estimated_bytes += estimated_bytes;
        self.entries.insert(
            key,
            FilterIndexEntry {
                ids,
                estimated_bytes,
                last_used,
            },
        );
    }

    fn invalidate(&mut self) {
        self.entries.clear();
        self.estimated_bytes = 0;
        self.invalidations += 1;
    }

    fn fallback(&mut self) {
        self.fallbacks += 1;
    }

    fn stats(&self) -> HashMap<String, u64> {
        HashMap::from([
            ("entries".into(), self.entries.len() as u64),
            ("bytes".into(), self.estimated_bytes as u64),
            ("hits".into(), self.hits),
            ("builds".into(), self.builds),
            ("evictions".into(), self.evictions),
            ("invalidations".into(), self.invalidations),
            ("fallbacks".into(), self.fallbacks),
            ("uncached".into(), self.uncached),
            ("max_entries".into(), FILTER_INDEX_MAX_ENTRIES as u64),
            ("max_fields".into(), FILTER_INDEX_MAX_FIELDS as u64),
            ("max_key_bytes".into(), FILTER_INDEX_MAX_KEY_BYTES as u64),
            (
                "max_ids_per_entry".into(),
                FILTER_INDEX_MAX_IDS_PER_ENTRY as u64,
            ),
            ("max_bytes".into(), FILTER_INDEX_MAX_BYTES as u64),
        ])
    }
}

impl CachedPool {
    fn unindex(&mut self, slot: u64, id: u64) {
        if let Some(ids) = self.slots.get_mut(&slot) {
            ids.remove(&id);
            if ids.is_empty() {
                self.slots.remove(&slot);
            }
        }
    }

    fn remove(&mut self, id: u64) -> bool {
        let Some(m) = self.members.remove(&id) else {
            return false;
        };
        if let Some(slot) = m.slot {
            self.unindex(slot, id);
        }
        true
    }

    pub fn apply(&mut self, d: &PoolDelta) {
        if d.full || !d.changed.is_empty() || !d.removed.is_empty() {
            *self.snapshots.get_mut().unwrap() = Default::default();
            *self.native.get_mut().unwrap() = Default::default();
            *self.digest.get_mut().unwrap() = None;
            self.filter_index.get_mut().unwrap().invalidate();
        }
        let mut membership_changed = d.full;
        let mut readiness_changed = false;
        if d.full {
            self.members.clear();
            self.slots.clear();
        }
        for m in &d.changed {
            let old = self.members.insert(m.id, Arc::new(m.clone()));
            membership_changed |= old.is_none();
            readiness_changed |= old.as_ref().map(|m| m.ready) != Some(m.ready);
            let old_slot = old.as_ref().and_then(|m| m.slot);
            if old_slot != m.slot {
                if let Some(slot) = old_slot {
                    self.unindex(slot, m.id);
                }
                if let Some(slot) = m.slot {
                    self.slots.entry(slot).or_default().insert(m.id);
                }
            }
        }
        for id in &d.removed {
            membership_changed |= self.remove(*id);
        }
        if membership_changed {
            self.ids.take();
        }
        if membership_changed || readiness_changed {
            self.ready_ids.take();
        }
        self.version = d.version;
        self.roster = d.roster;
        if self.methods.as_ref() != d.methods.as_slice() {
            self.methods = Arc::from(d.methods.clone());
        }
        self.size = d.size;
    }

    pub fn ids(&self, require_ready: bool) -> &[u64] {
        let ids = self.ids.get_or_init(|| {
            let mut ids: Vec<u64> = self.members.keys().copied().collect();
            ids.sort_unstable();
            ids
        });
        if require_ready {
            self.ready_ids.get_or_init(|| {
                ids.iter()
                    .copied()
                    .filter(|id| self.members[id].ready)
                    .collect()
            })
        } else {
            ids
        }
    }

    fn scan_ids(&self, filter: &serde_json::Value, require_ready: bool) -> Vec<u64> {
        self.ids(require_ready)
            .iter()
            .copied()
            .filter(|id| self.members[id].matches(filter))
            .collect()
    }

    fn matching_ids(&self, filter: &serde_json::Value, require_ready: bool) -> Option<Arc<[u64]>> {
        let Some((key, key_bytes)) = IndexedFilterKey::from_filter(filter, require_ready) else {
            self.filter_index.lock().unwrap().fallback();
            return None;
        };
        if let Some(ids) = self.filter_index.lock().unwrap().get(&key) {
            return Some(ids);
        }
        let ids: Arc<[u64]> = Arc::from(self.scan_ids(filter, require_ready));
        self.filter_index
            .lock()
            .unwrap()
            .insert(key, key_bytes, ids.clone());
        Some(ids)
    }

    pub fn filter_index_stats(&self) -> HashMap<String, u64> {
        self.filter_index.lock().unwrap().stats()
    }

    pub fn clear_filter_index(&self) {
        self.filter_index.lock().unwrap().invalidate();
    }

    pub fn debug_scan_ids(&self, filter: &serde_json::Value, require_ready: bool) -> Vec<u64> {
        if filter.as_object().is_none_or(|fields| fields.is_empty()) {
            return self.ids(require_ready).to_vec();
        }
        self.scan_ids(filter, require_ready)
    }

    pub fn slot(&self, slot: u64, require_ready: bool) -> Option<&Member> {
        self.slots.get(&slot)?.iter().find_map(|id| {
            let m = self.members[id].as_ref();
            (!require_ready || m.ready).then_some(m)
        })
    }

    pub fn get(&self, pool: &str, identity: &str) -> Option<&Member> {
        let (named_pool, key, incarnation) = identity_parts(identity)?;
        if named_pool != pool {
            return None;
        }
        if let Some(ids) = self.slots.get(&key) {
            if let Some(member) = ids.iter().find_map(|id| {
                let member = self.members[id].as_ref();
                (member.incarnation == incarnation).then_some(member)
            }) {
                return Some(member);
            }
        }
        let member = self.members.get(&key)?.as_ref();
        (member.slot.is_none() && member.incarnation == incarnation).then_some(member)
    }

    pub fn choose(
        &self,
        filter: &serde_json::Value,
        require_ready: bool,
        rng: &mut fastrand::Rng,
    ) -> Option<&Member> {
        if filter.as_object().is_none_or(|f| f.is_empty()) {
            let ids = self.ids(require_ready);
            return (!ids.is_empty()).then(|| self.members[&ids[rng.usize(..ids.len())]].as_ref());
        }
        if let Some(ids) = self.matching_ids(filter, require_ready) {
            return (!ids.is_empty()).then(|| self.members[&ids[rng.usize(..ids.len())]].as_ref());
        }
        let mut chosen = None;
        let mut count = 0;
        for m in self.members.values() {
            if (!require_ready || m.ready) && m.matches(filter) {
                count += 1;
                if rng.usize(..count) == 0 {
                    chosen = Some(m.as_ref());
                }
            }
        }
        chosen
    }

    pub fn choose_arc(
        &self,
        filter: &serde_json::Value,
        require_ready: bool,
        rng: &mut fastrand::Rng,
    ) -> Option<Arc<Member>> {
        if filter.as_object().is_none_or(|fields| fields.is_empty()) {
            let ids = self.ids(require_ready);
            return (!ids.is_empty()).then(|| self.members[&ids[rng.usize(..ids.len())]].clone());
        }
        if let Some(ids) = self.matching_ids(filter, require_ready) {
            return (!ids.is_empty()).then(|| self.members[&ids[rng.usize(..ids.len())]].clone());
        }
        let mut chosen = None;
        let mut count = 0;
        for member in self.members.values() {
            if (!require_ready || member.ready) && member.matches(filter) {
                count += 1;
                if rng.usize(..count) == 0 {
                    chosen = Some(member.clone());
                }
            }
        }
        chosen
    }

    pub fn slot_owned(&self, slot: u64, require_ready: bool) -> Option<Arc<Member>> {
        self.slots.get(&slot)?.iter().find_map(|id| {
            let member = &self.members[id];
            (!require_ready || member.ready).then(|| member.clone())
        })
    }

    pub fn count(&self, filter: &serde_json::Value, require_ready: bool) -> usize {
        if filter.as_object().is_none_or(|fields| fields.is_empty()) {
            return self.ids(require_ready).len();
        }
        if let Some(ids) = self.matching_ids(filter, require_ready) {
            return ids.len();
        }
        self.members
            .values()
            .filter(|member| (!require_ready || member.ready) && member.matches(filter))
            .count()
    }

    fn make_native<I>(&self, members: I) -> Arc<FrozenMembers>
    where
        I: IntoIterator<Item = Arc<Member>>,
    {
        Arc::new(FrozenMembers::new(members.into_iter().collect()))
    }

    pub fn native(&self, require_ready: bool) -> Arc<FrozenMembers> {
        let mut native = self.native.lock().unwrap();
        let cached = &mut native[usize::from(require_ready)];
        cached
            .get_or_insert_with(|| {
                self.make_native(
                    self.ids(require_ready)
                        .iter()
                        .map(|id| self.members[id].clone()),
                )
            })
            .clone()
    }

    pub fn frozen(&self, require_ready: bool) -> FrozenPool {
        FrozenPool {
            members: self.native(require_ready),
            roster: self.roster,
            version: self.version,
            size: self.size,
            methods: self.methods.clone(),
        }
    }

    pub fn filtered(&self, filter: &serde_json::Value, require_ready: bool) -> FrozenPool {
        if filter.as_object().is_none_or(|fields| fields.is_empty()) {
            return self.frozen(require_ready);
        }
        if let Some(ids) = self.matching_ids(filter, require_ready) {
            return FrozenPool {
                members: self.make_native(ids.iter().map(|id| self.members[id].clone())),
                roster: self.roster,
                version: self.version,
                size: self.size,
                methods: self.methods.clone(),
            };
        }
        FrozenPool {
            members: self.make_native(
                self.ids(require_ready)
                    .iter()
                    .map(|id| self.members[id].clone())
                    .filter(|member| member.matches(filter)),
            ),
            roster: self.roster,
            version: self.version,
            size: self.size,
            methods: self.methods.clone(),
        }
    }

    pub fn serialized(&self, require_ready: bool) -> (Vec<u8>, u64) {
        let mut snapshots = self.snapshots.lock().unwrap();
        let cached = &mut snapshots[usize::from(require_ready)];
        if let Some(cached) = cached {
            return (cached.bytes.clone(), cached.roster);
        }
        let members: Vec<&Member> = self
            .ids(require_ready)
            .iter()
            .map(|id| self.members[id].as_ref())
            .collect();
        let roster = members.iter().fold(0, |h, m| h ^ m.roster_hash());
        let bytes =
            rmp_serde::to_vec_named(&members).expect("members always encode as MessagePack");
        if bytes.len() <= SNAPSHOT_BYTES {
            *cached = Some(SerializedMembers {
                bytes: bytes.clone(),
                roster,
            });
        }
        (bytes, roster)
    }

    pub fn field_digest(&self, fields: &[String]) -> u64 {
        let mut cached = self.digest.lock().unwrap();
        if let Some((keys, digest)) = cached.as_ref() {
            if keys == fields {
                return *digest;
            }
        }
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for id in self.ids(false) {
            let m = self.members[id].as_ref();
            m.id.hash(&mut h);
            m.incarnation.hash(&mut h);
            for f in fields {
                match f.as_str() {
                    "ready" => m.ready.hash(&mut h),
                    "url" => m.url.hash(&mut h),
                    other => m
                        .state
                        .get(other)
                        .map(|v| v.to_string())
                        .unwrap_or_default()
                        .hash(&mut h),
                }
            }
        }
        let digest = h.finish();
        // A caller-controlled list of fields must not become an unbounded cache.
        if fields.len() <= 64 && fields.iter().map(String::len).sum::<usize>() <= 4096 {
            *cached = Some((fields.to_vec(), digest));
        }
        digest
    }
}
