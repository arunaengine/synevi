use ahash::RandomState;
use heed::{
    byteorder::BigEndian,
    types::{SerdeBincode, U128},
    Database, Env, EnvFlags, EnvOpenOptions,
};
use monotime::MonoTime;
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex},
};
use synevi_types::{
    error::SyneviError,
    traits::Store,
    types::{Event, Hashes, RecoverDependencies, RecoverEvent, UpsertEvent},
    Ballot, State, T, T0,
};
use tokio::sync::mpsc::Receiver;
use tracing::instrument;

const EVENT_DB_NAME: &str = "events";
const ID_MAPPINGS_DB_NAME: &str = "id_mappings";
type EventDb = Database<U128<BigEndian>, SerdeBincode<Event>>;

#[derive(Clone, Debug)]
pub struct LmdbStore {
    data: Arc<Mutex<InternalData>>,
}

#[derive(Clone, Debug)]
struct InternalData {
    db: Env,
    events: EventDb,
    id_mappings: Database<U128<BigEndian>, U128<BigEndian>>,
    pub(crate) mappings: BTreeMap<T, T0>, // Key: t, value t0
    pub last_applied: T,                  // t of last applied entry
    pub(crate) latest_time: MonoTime,     // last created or recognized t0
    pub node_serial: u16,
    latest_hash: [u8; 32],
}

impl LmdbStore {
    pub fn new(path: String, node_serial: u16) -> Result<LmdbStore, SyneviError> {
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024)
                .max_dbs(16)
                .flags(EnvFlags::MAP_ASYNC | EnvFlags::WRITE_MAP)
                .open(path)?
        };
        let env_clone = env.clone();
        let mut write_txn = env.write_txn()?;
        let events_db: Option<EventDb> = env.open_database(&write_txn, Some(EVENT_DB_NAME))?;
        let id_mappings: Database<_, _> =
            env.create_database(&mut write_txn, Some(ID_MAPPINGS_DB_NAME))?;
        match events_db {
            Some(db) => {
                let result = db
                    .iter(&write_txn)?
                    .filter_map(|e| {
                        if let Ok((_, event)) = e {
                            Some(event)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                let mut mappings = BTreeMap::default();
                let mut last_applied = T::default();
                let mut latest_time = MonoTime::default();
                let mut latest_hash: [u8; 32] = [0; 32];
                for event in result {
                    mappings.insert(event.t, event.t_zero);
                    if event.state == State::Applied && event.t > last_applied {
                        last_applied = event.t;
                        latest_hash = if let Some(hashes) = event.hashes {
                            hashes.transaction_hash
                        } else {
                            return Err(SyneviError::MissingTransactionHash);
                        };
                    }
                    if *event.t > latest_time {
                        latest_time = *event.t;
                    }
                }
                write_txn.commit()?;
                Ok(LmdbStore {
                    //db: env_clone,
                    data: Arc::new(Mutex::new(InternalData {
                        db: env_clone,
                        events: db,
                        id_mappings,
                        mappings,
                        last_applied,
                        latest_time,
                        node_serial,
                        latest_hash,
                    })),
                })
            }
            None => {
                let events: EventDb = env.create_database(&mut write_txn, Some(EVENT_DB_NAME))?;
                write_txn.commit()?;
                Ok(LmdbStore {
                    data: Arc::new(Mutex::new(InternalData {
                        db: env_clone,
                        events,
                        id_mappings,
                        mappings: BTreeMap::default(),
                        last_applied: T::default(),
                        latest_time: MonoTime::default(),
                        node_serial,
                        latest_hash: [0; 32],
                    })),
                })
            }
        }
    }
}

impl Store for LmdbStore {
    #[instrument(level = "trace")]
    fn init_t_zero(&self, node_serial: u16) -> T0 {
        self.data
            .lock()
            .expect("poisoned lock, aborting")
            .init_t_zero(node_serial)
    }

    #[instrument(level = "trace")]
    fn pre_accept_tx(
        &self,
        id: u128,
        t_zero: T0,
        transaction: Vec<u8>,
    ) -> Result<(T, HashSet<T0, RandomState>), SyneviError> {
        self.data
            .lock()
            .expect("poisoned lock, aborting")
            .pre_accept_tx(id, t_zero, transaction)
    }

    #[instrument(level = "trace")]
    fn get_tx_dependencies(&self, t: &T, t_zero: &T0) -> HashSet<T0, RandomState> {
        self.data
            .lock()
            .expect("poisoned lock, aborting")
            .get_tx_dependencies(t, t_zero)
    }

    #[instrument(level = "trace")]
    fn accept_tx_ballot(&self, t_zero: &T0, ballot: Ballot) -> Option<Ballot> {
        self.data
            .lock()
            .expect("poisoned lock, aborting")
            .accept_tx_ballot(t_zero, ballot)
    }

    #[instrument(level = "trace", skip(self))]
    fn upsert_tx(&self, upsert_event: UpsertEvent) -> Result<(), SyneviError> {
        self.data
            .lock()
            .expect("poisoned lock, aborting")
            .upsert_tx(upsert_event)
    }

    #[instrument(level = "trace")]
    fn get_recover_deps(&self, t_zero: &T0) -> Result<RecoverDependencies, SyneviError> {
        self.data
            .lock()
            .expect("poisoned lock, aborting")
            .get_recover_deps(t_zero)
    }

    #[instrument(level = "trace")]
    fn get_event_state(&self, t_zero: &T0) -> Option<State> {
        self.data
            .lock()
            .expect("poisoned lock, aborting")
            .get_event_state(t_zero)
    }

    #[instrument(level = "trace")]
    fn recover_event(
        &self,
        t_zero_recover: &T0,
        node_serial: u16,
    ) -> Result<Option<RecoverEvent>, SyneviError> {
        self.data
            .lock()
            .expect("poisoned lock, aborting")
            .recover_event(t_zero_recover, node_serial)
    }

    #[instrument(level = "trace")]
    fn get_event_store(&self) -> BTreeMap<T0, Event> {
        self.data
            .lock()
            .expect("poisoned lock, aborting")
            .get_event_store()
    }

    #[instrument(level = "trace")]
    fn last_applied(&self) -> (T, T0) {
        self.data
            .lock()
            .expect("poisoned lock, aborting")
            .last_applied()
    }

    #[instrument(level = "trace")]
    fn get_events_after(
        &self,
        last_applied: T,
    ) -> Result<Receiver<Result<Event, SyneviError>>, SyneviError> {
        self.data
            .lock()
            .expect("poisoned lock, aborting")
            .get_events_after(last_applied)
    }

    #[instrument(level = "trace", skip(self))]
    fn get_event(&self, t_zero: T0) -> Result<Option<Event>, SyneviError> {
        self.data
            .lock()
            .expect("poisoned lock, aborting")
            .get_event(t_zero)
    }

    fn inc_time_with_guard(&self, guard: T0) -> Result<(), SyneviError> {
        let mut lock = self.data.lock().expect("poisoned lock, aborting");
        lock.latest_time = lock
            .latest_time
            .next_with_guard_and_node(&guard, lock.node_serial)
            .into_time();
        Ok(())
    }

    fn get_or_update_transaction_hash(&self, event: UpsertEvent) -> Result<Hashes, SyneviError> {
        let lock = self.data.lock().expect("poisoned lock, aborting");
        if let Some(event) = lock.get_event(event.t_zero)? {
            if event.state == State::Applied {
                if let Some(hashes) = event.hashes {
                    return Ok(hashes);
                }
            }
        }
        let mut event = Event::from(event);
        event.state = State::Applied;
        Ok(event.hash_event(lock.latest_hash))
    }

    fn get_event_by_id(&self, id: u128) -> Result<Option<Event>, SyneviError> {
        let lock = self.data.lock().expect("poisoned lock, aborting");
        let read_txn = lock.db.read_txn()?;
        let Some(mapping) = lock.id_mappings.get(&read_txn, &id)? else {
            return Ok(None);
        };
        lock.events.get(&read_txn, &mapping).map_err(Into::into)
    }
}

impl InternalData {
    #[instrument(level = "trace")]
    fn init_t_zero(&mut self, node_serial: u16) -> T0 {
        let next_time = self.latest_time.next_with_node(node_serial).into_time();
        self.latest_time = next_time;
        T0(next_time)
    }

    #[instrument(level = "trace")]
    fn pre_accept_tx(
        &mut self,
        id: u128,
        t_zero: T0,
        transaction: Vec<u8>,
    ) -> Result<(T, HashSet<T0, RandomState>), SyneviError> {
        let (t, deps) = {
            let t = if self.latest_time > *t_zero {
                let new_time_t = t_zero
                    .next_with_guard_and_node(&self.latest_time, self.node_serial)
                    .into_time();

                self.latest_time = new_time_t;
                T(new_time_t)
            } else {
                T(*t_zero)
            };
            // This might not be necessary to re-use the write lock here
            let deps = self.get_tx_dependencies(&t, &t_zero);
            (t, deps)
        };

        let event = UpsertEvent {
            id,
            t_zero,
            t,
            state: State::PreAccepted,
            transaction: Some(transaction),
            dependencies: Some(deps.clone()),
            ..Default::default()
        };
        self.upsert_tx(event)?;
        //self.db.force_sync()?;
        Ok((t, deps))
    }

    #[instrument(level = "trace")]
    fn get_tx_dependencies(&self, t: &T, t_zero: &T0) -> HashSet<T0, RandomState> {
        if self.last_applied == *t {
            return HashSet::default();
        }
        assert!(self.last_applied < *t);
        let mut deps = HashSet::default();
        if let Some(last_applied_t0) = self.mappings.get(&self.last_applied) {
            if last_applied_t0 != &T0::default() {
                deps.insert(*last_applied_t0);
            }
        }
        // What about deps with dep_t0 < last_applied_t0 && dep_t > t?

        // Dependencies are where any of these cases match:
        // - t_dep < t if not applied
        // - t0_dep < t0_last_applied, if t_dep > t0
        // - t_dep > t if t0_dep < t
        for (_, t0_dep) in self.mappings.range(self.last_applied..) {
            if t0_dep != t_zero && (t0_dep < &T0(**t)) {
                deps.insert(*t0_dep);
            }
        }
        deps
    }

    #[instrument(level = "trace")]
    fn accept_tx_ballot(&self, t_zero: &T0, ballot: Ballot) -> Option<Ballot> {
        let mut write_txn = self.db.write_txn().ok()?;
        let mut event = self.events.get(&write_txn, &t_zero.get_inner()).ok()??;

        if event.ballot < ballot {
            event.ballot = ballot;
            let _ = self.events.put(&mut write_txn, &t_zero.get_inner(), &event);
        }
        write_txn.commit().ok()?;
        //self.db.force_sync().ok()?;

        Some(event.ballot)
    }

    #[instrument(level = "trace", skip(self))]
    fn upsert_tx(&mut self, upsert_event: UpsertEvent) -> Result<(), SyneviError> {
        //let db = self.db.clone();

        // Update the latest time
        if self.latest_time < *upsert_event.t {
            self.latest_time = *upsert_event.t;
        }

        let mut write_txn = self.db.write_txn()?;
        let events_db: EventDb = self.events;

        let event = events_db.get(&write_txn, &upsert_event.t_zero.get_inner())?;

        let Some(mut event) = event else {
            let mut event = Event::from(upsert_event.clone());

            // Not an update -> Add events id mapping
            self.id_mappings.put(
                &mut write_txn,
                &upsert_event.id,
                &upsert_event.t_zero.get_inner(),
            )?;

            if matches!(event.state, State::Applied) {
                self.mappings.insert(event.t, event.t_zero);
                if let Some(deps) = upsert_event.dependencies {
                    event.dependencies = deps;
                }
                if let Some(transaction) = upsert_event.transaction {
                    if event.transaction.is_empty() && !transaction.is_empty() {
                        event.transaction = transaction;
                    }
                }
                event.state = upsert_event.state;
                if let Some(ballot) = upsert_event.ballot {
                    if event.ballot < ballot {
                        event.ballot = ballot;
                    }
                }

                let last_t = self.last_applied;

                // Safeguard
                assert!(last_t < event.t);

                self.last_applied = event.t;
                let hashes = upsert_event
                    .hashes
                    .ok_or_else(|| SyneviError::MissingExecutionHash)?;
                self.latest_hash = hashes.transaction_hash;
                event.hashes = Some(hashes.clone());

                events_db.put(&mut write_txn, &upsert_event.t_zero.get_inner(), &event)?;
            } else {
                events_db.put(&mut write_txn, &upsert_event.t_zero.get_inner(), &event)?;
                self.mappings.insert(upsert_event.t, upsert_event.t_zero);
            }
            write_txn.commit()?;
            return Ok(());
        };

        // Do not update to a "lower" state
        if upsert_event.state < event.state {
            write_txn.commit()?;
            return Ok(());
        }

        // Event is already applied
        if event.state == State::Applied {
            write_txn.commit()?;
            return Ok(());
        }

        if event.is_update(&upsert_event) {
            if let Some(old_t) = event.update_t(upsert_event.t) {
                self.mappings.remove(&old_t);
                self.mappings.insert(event.t, event.t_zero);
            }
            if let Some(deps) = upsert_event.dependencies {
                event.dependencies = deps;
            }
            if let Some(transaction) = upsert_event.transaction {
                if event.transaction.is_empty() && !transaction.is_empty() {
                    event.transaction = transaction;
                }
            }
            event.state = upsert_event.state;
            if let Some(ballot) = upsert_event.ballot {
                if event.ballot < ballot {
                    event.ballot = ballot;
                }
            }

            if event.state == State::Applied {
                let last_t = self.last_applied;

                if last_t > event.t {
                    println!("last_t: {:?}, event.t: {:?}", last_t, event.t);
                }
                assert!(last_t < event.t);

                self.last_applied = event.t;
                let hashes = upsert_event
                    .hashes
                    .ok_or_else(|| SyneviError::MissingExecutionHash)?;
                self.latest_hash = hashes.transaction_hash;
                event.hashes = Some(hashes.clone());
            };
            events_db.put(&mut write_txn, &upsert_event.t_zero.get_inner(), &event)?;
            write_txn.commit()?;
            Ok(())
        } else {
            write_txn.commit()?;
            Ok(())
        }
    }

    #[instrument(level = "trace")]
    fn get_recover_deps(&self, t_zero: &T0) -> Result<RecoverDependencies, SyneviError> {
        let read_txn = self.db.read_txn()?;
        let timestamp = self
            .events
            .get(&read_txn, &t_zero.get_inner())?
            .ok_or_else(|| SyneviError::EventNotFound(t_zero.get_inner()))?
            .t;
        let mut recover_deps = RecoverDependencies {
            timestamp,
            ..Default::default()
        };

        for (t_dep, t_zero_dep) in self.mappings.range(self.last_applied..) {
            let dep_event = self
                .events
                .get(&read_txn, &t_zero_dep.get_inner())?
                .ok_or_else(|| SyneviError::DependencyNotFound(t_zero_dep.get_inner()))?;
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
                State::Committed => {
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

    fn get_event_state(&self, t_zero: &T0) -> Option<State> {
        let read_txn = self.db.read_txn().ok()?;
        let state = self
            .events
            .get(&read_txn, &t_zero.get_inner())
            .ok()?
            .ok_or_else(|| SyneviError::EventNotFound(t_zero.get_inner()))
            .ok()?
            .state;
        Some(state)
    }

    fn recover_event(
        &self,
        t_zero_recover: &T0,
        node_serial: u16,
    ) -> Result<Option<RecoverEvent>, SyneviError> {
        let Some(state) = self.get_event_state(t_zero_recover) else {
            return Ok(None);
        };
        if matches!(state, synevi_types::State::Undefined) {
            return Err(SyneviError::UndefinedRecovery);
        }

        let mut write_txn = self.db.write_txn()?;
        let event = self.events.get(&write_txn, &t_zero_recover.get_inner())?;

        if let Some(mut event) = event {
            event.ballot = Ballot(event.ballot.next_with_node(node_serial).into_time());
            self.events
                .put(&mut write_txn, &t_zero_recover.get_inner(), &event)?;
            write_txn.commit()?;

            Ok(Some(RecoverEvent {
                id: event.id,
                t_zero: event.t_zero,
                t: event.t,
                state,
                transaction: event.transaction.clone(),
                dependencies: event.dependencies.clone(),
                ballot: event.ballot,
            }))
        } else {
            write_txn.commit()?;
            Ok(None)
        }
    }

    fn get_event_store(&self) -> BTreeMap<T0, Event> {
        // TODO: Remove unwrap and change trait result
        let read_txn = self.db.read_txn().unwrap();
        let result = self
            .events
            .iter(&read_txn)
            .unwrap()
            .filter_map(|e| {
                if let Ok((t0, event)) = e {
                    Some((T0(MonoTime::from(t0)), event))
                } else {
                    None
                }
            })
            .collect::<BTreeMap<T0, Event>>();
        result
    }

    fn last_applied(&self) -> (T, T0) {
        let t = self.last_applied;
        let t0 = self.mappings.get(&t).cloned().unwrap_or(T0::default());
        (t, t0)
    }

    fn get_events_after(
        &self,
        last_applied: T,
    ) -> Result<Receiver<Result<Event, SyneviError>>, SyneviError> {
        let (sdx, rcv) = tokio::sync::mpsc::channel(200);
        let db = self.db.clone();
        let last_applied_t0 = match self.mappings.get(&last_applied) {
            Some(t0) => *t0,
            None if last_applied == T::default() => T0::default(),
            _ => return Err(SyneviError::EventNotFound(last_applied.get_inner())),
        };

        let events_db = self.events;
        tokio::task::spawn_blocking(move || {
            let read_txn = db.read_txn()?;
            let range = last_applied_t0.get_inner()..;
            for result in events_db.range(&read_txn, &range)? {
                let (_t0, event) = result?;
                sdx.blocking_send(Ok(event))
                    .map_err(|e| SyneviError::SendError(e.to_string()))?;
            }
            Ok::<(), SyneviError>(())
        });
        Ok(rcv)
    }

    fn get_event(&self, t_zero: T0) -> Result<Option<Event>, SyneviError> {
        let read_txn = self.db.read_txn()?;
        let event = self.events.get(&read_txn, &t_zero.get_inner())?;
        Ok(event)
    }
}
