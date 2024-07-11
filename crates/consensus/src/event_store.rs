use crate::{
    coordinator::TransactionStateMachine,
    utils::{from_dependency, Ballot, T, T0},
};
use ahash::RandomState;
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use consensus_transport::consensus_transport::State;
use monotime::MonoTime;
use persistence::{Database, SplitEvent};
use std::collections::{BTreeMap, HashSet};
use tracing::instrument;

#[derive(Debug)]
pub struct EventStore {
    // Has both temp and persisted data
    // pros:
    //  - Only iterate once for deps
    //  - Reordering separate from events
    //  - RwLock allows less blocking for read deps
    // cons:
    //  - Updates need to consider both maps (when t is changing)
    //  - Events and mappings need clean up cron jobs and some form of consolidation
    pub events: BTreeMap<T0, Event>, // Key: t0, value: Event
    pub database: Option<Database>,
    pub(crate) mappings: BTreeMap<T, T0>, // Key: t, value t0
    pub last_applied: T,                  // t of last applied entry
    pub(crate) latest_t0: T0,             // last created or recognized t0
    pub node_serial: u16,
}

#[derive(Clone, Debug, Default)]
pub struct Event {
    pub t_zero: T0,
    pub t: T,

    // This holds the state and can be used by waiters to watch for a state change
    // In contrast to notify this can also be used if the state is already reached
    pub state: State,
    pub event: Vec<u8>,
    pub dependencies: HashSet<T0, RandomState>, // t and t_zero
    pub ballot: Ballot,
    //TODO: timestamp: Last updated
}

impl Event {
    pub fn as_bytes(&self) -> Bytes {
        let mut new: BytesMut = BytesMut::new();

        new.put(<[u8; 16]>::from(*self.t).as_slice());
        let state: i32 = self.state.into();
        new.put(state.to_be_bytes().as_slice()); // -> [u8: 4]
        new.put(<[u8; 16]>::from(*self.ballot).as_slice());

        for dep in &self.dependencies {
            new.put::<Bytes>((*dep).into());
        }

        new.freeze()
    }
    pub fn from_bytes(input: SplitEvent) -> Result<Self> {
        let mut state = input.state;
        let mut event = Event::default();
        event.t_zero = T0(MonoTime::try_from(input.key.as_ref())?);
        event.event = input.event.into();
        event.t = T::try_from(state.split_to(16))?;
        event.state = State::try_from(i32::from_be_bytes(<[u8; 4]>::try_from(
            state.split_to(4).iter().as_slice(),
        )?))?;
        event.ballot = Ballot::try_from(state.split_to(16))?;
        while !state.is_empty() {
            let dep = state.split_to(16);
            let t0_dep = T0::try_from(dep)?;
            event.dependencies.insert(t0_dep);
        }
        Ok(event)
    }
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.t_zero == other.t_zero
            && self.t == other.t
            && self.state == other.state
            && self.event == other.event
            && self.dependencies == other.dependencies
            && self.ballot == other.ballot
    }
}

#[derive(Debug, Default)]
pub(crate) struct RecoverDependencies {
    pub dependencies: HashSet<T0, RandomState>,
    pub wait: HashSet<T0, RandomState>,
    pub superseding: bool,
}

impl EventStore {
    #[instrument(level = "trace")]
    pub fn init(path: Option<String>, node_serial: u16) -> Result<Self> {
        match path {
            Some(path) => {
                // TODO: Read all from DB and fill event store
                let mut events = BTreeMap::default();
                let mut mappings = BTreeMap::default();
                let mut last_applied = T::default();
                let mut latest_t0 = T0::default();
                let db = Database::new(path)?;
                let result = db.read_all()?;
                for entry in result {
                    let event = Event::from_bytes(entry)?;
                    if event.state == State::Applied && event.t > last_applied {
                        last_applied = event.t;
                    }
                    if latest_t0 < event.t_zero {
                        latest_t0 = event.t_zero;
                    }
                    mappings.insert(event.t, event.t_zero);
                    events.insert(event.t_zero, event);
                }
                Ok(EventStore {
                    events,
                    mappings,
                    last_applied,
                    latest_t0,
                    database: Some(db),
                    node_serial,
                })
            }
            None => Ok(EventStore {
                events: BTreeMap::default(),
                mappings: BTreeMap::default(),
                last_applied: T::default(),
                latest_t0: T0::default(),
                database: None,
                node_serial,
            }),
        }
    }

    #[instrument(level = "trace")]
    async fn insert(&mut self, event: Event) {
        self.events.insert(event.t_zero, event.clone());
        self.mappings.insert(event.t, event.t_zero);
    }

    #[instrument(level = "trace")]
    pub async fn init_transaction(
        &mut self,
        body: Vec<u8>,
        node_serial: u16,
    ) -> TransactionStateMachine {
        let t0 = self.latest_t0.next_with_node(node_serial).into_time();
        TransactionStateMachine {
            state: State::PreAccepted,
            transaction: body,
            t_zero: T0(t0),
            t: T(t0),
            dependencies: HashSet::default(),
            ballot: Ballot::default(),
        }
    }

    #[instrument(level = "trace")]
    pub async fn get_event(&self, t_zero: T0) -> Option<Event> {
        self.events.get(&t_zero).cloned()
    }

    #[instrument(level = "trace")]
    pub async fn get_or_insert(&mut self, t_zero: T0) -> Event {
        let entry = self.events.entry(t_zero).or_insert(Event {
            t_zero,
            t: T(*t_zero),
            state: State::Undefined,
            event: Default::default(),
            dependencies: HashSet::default(),
            ballot: Ballot::default(),
        });
        entry.clone()
    }

    #[instrument(level = "trace")]
    pub async fn pre_accept(&mut self, t_zero: T0, transaction: Vec<u8>) -> Result<(Vec<u8>, T)> {
        let (t, deps) = {
            let t = T(if let Some((last_t, _)) = self.mappings.last_key_value() {
                if **last_t > *t_zero {
                    // This unwrap will not panic
                    t_zero
                        .next_with_guard_and_node(last_t, self.node_serial)
                        .into_time()
                } else {
                    *t_zero
                }
            } else {
                // No entries in the map -> insert the new event
                *t_zero
            });
            // This might not be necessary to re-use the write lock here
            let deps: Vec<u8> = self.get_dependencies(&t, &t_zero).await;
            (t, deps)
        };

        // This is OK because on pre_accept T == T0

        let event = Event {
            t_zero,
            t,
            state: State::PreAccepted,
            event: transaction,
            dependencies: from_dependency(deps.clone())?,
            ballot: Ballot::default(),
        };
        self.upsert(event).await;
        Ok((deps, t))
    }

    #[instrument(level = "trace")]
    pub fn get_ballot(&self, t_zero: &T0) -> Ballot {
        self.events
            .get(t_zero)
            .map(|event| event.ballot)
            .unwrap_or_default()
    }
    #[instrument(level = "trace")]
    pub fn update_ballot(&mut self, t_zero: &T0, ballot: Ballot) {
        if let Some(event) = self.events.get_mut(t_zero) {
            event.ballot = ballot;
        }
    }

    #[instrument(level = "trace")]
    pub async fn upsert(&mut self, event: Event) {
        let old_event = self.events.entry(event.t_zero).or_insert(event.clone());
        if self.latest_t0 < event.t_zero {
            self.latest_t0 = event.t_zero;
        }

        //println!("T0: {:?}, Old: {:?} new: {:?} @ {}", event.t_zero, old_event.state, event.state, self.node_serial);
        if event.state < old_event.state {
            return;
        }

        // assert!(&old_event.t <= &event.t ); Can happen if minority is left behind
        let mut update = false;
        if old_event != &event {
            update = true;
            self.mappings.remove(&old_event.t);
            old_event.t = event.t;
            old_event.dependencies = event.dependencies;
            old_event.event = event.event;
            old_event.state = event.state;
            if event.ballot > old_event.ballot {
                old_event.ballot = event.ballot;
            }
        }

        self.mappings.insert(event.t, event.t_zero);
        if old_event.state == State::Applied {
            self.last_applied = event.t;
        }

        if let Some(db) = &self.database {
            let t: Vec<u8> = (*old_event.t).into();
            if update {
                db.update_object(t.into(), old_event.as_bytes())
                    .await
                    .unwrap()
            } else {
                db.init_object(
                    t.into(),
                    Bytes::from(old_event.event.clone()),
                    old_event.as_bytes(),
                )
                .await
                .unwrap()
            }
        }
    }

    #[instrument(level = "trace")]
    pub async fn get_dependencies(&self, t: &T, t_zero: &T0) -> Vec<u8> {
        let mut deps = Vec::new();
        if self.last_applied == T::default() {
            for (t0, event) in self.events.range(..&T0(**t)) {
                if event.state != State::Undefined && t0 != t_zero {
                    deps.put::<Bytes>((*t0).into());
                }
            }
        } else if let Some(last_t0) = self.mappings.get(&self.last_applied) {
            if **last_t0 != **t {
                // Range from last applied t0 to T
                // -> Get all T0s that are before our T
                for (t0, event) in self.events.range(last_t0..&T0(**t)) {
                    if event.state != State::Undefined && t0 != t_zero {
                        deps.put::<Bytes>((*t0).into());
                    }
                }
            }
        }
        deps
    }

    #[instrument(level = "trace")]
    pub async fn get_recover_deps(&self, t: &T, t_zero: &T0) -> Result<RecoverDependencies> {
        let mut recover_deps = RecoverDependencies::default();
        for (t_dep, t_zero_dep) in self.mappings.range(self.last_applied..) {
            let dep_event = self.events.get(t_zero_dep).ok_or_else(|| {
                anyhow::anyhow!("Dependency not found for t_zero: {:?}", t_zero_dep)
            })?;
            match dep_event.state {
                State::Accepted => {
                    if dep_event
                        .dependencies
                        .iter()
                        .any(|t_zero_dep_dep| t_zero == t_zero_dep_dep)
                    {
                        // Wait -> Accord p19 l7 + l9
                        if t_zero_dep < t_zero && **t_dep > **t_zero {
                            recover_deps.wait.insert(*t_zero_dep);
                        }
                        // Superseding -> Accord: p19 l10
                        if t_zero_dep > t_zero {
                            recover_deps.superseding = true;
                        }
                    }
                }
                State::Commited => {
                    if dep_event
                        .dependencies
                        .iter()
                        .any(|t_zero_dep_dep| t_zero == t_zero_dep_dep)
                    {
                        // Superseding -> Accord: p19 l11
                        if **t_dep > **t_zero {
                            recover_deps.superseding = true;
                        }
                    }
                }
                _ => {}
            }
            // Collect "normal" deps -> Accord: p19 l16
            if t_zero_dep < t_zero {
                recover_deps.dependencies.insert(*t_zero_dep);
            }
        }
        Ok(recover_deps)
    }
}

#[cfg(test)]
mod tests {
    use crate::event_store::Event;
    use crate::utils::{Ballot, T, T0};
    use bytes::Bytes;
    use consensus_transport::consensus_transport::State;
    use monotime::MonoTime;
    use persistence::SplitEvent;
    use std::collections::HashSet;

    #[test]
    fn event_conversion() {
        let mut dependencies = HashSet::default();
        for i in 0..3 {
            dependencies.insert(T0(MonoTime::new(0, i)));
        }
        let t_zero = T0(MonoTime::new(1, 1));
        let t = T(t_zero.next().into_time());

        let event = Event {
            t_zero,
            t,
            state: State::Commited,
            event: Vec::from(b"this is a test transaction"),
            dependencies,
            ballot: Ballot(MonoTime::new(1, 1)),
        };
        let key = Bytes::from(t_zero.0);
        let payload = Bytes::from(event.event.clone());
        let state = event.as_bytes();
        let event_to_bytes = SplitEvent {
            key,
            event: payload,
            state,
        };

        let back_to_struct = Event::from_bytes(event_to_bytes).unwrap();

        assert_eq!(event, back_to_struct)
    }
}
