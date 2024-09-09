use ahash::RandomState;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use synevi_types::error::SyneviError;
use synevi_types::traits::Store;
use synevi_types::types::RecoverEvent;
use synevi_types::types::{Event, Hashes, RecoverDependencies, UpsertEvent};
use synevi_types::State;
use synevi_types::{Ballot, T, T0};
use tracing::instrument;

#[derive(Debug)]
pub struct MemStore {
    pub events: BTreeMap<T0, Event>,      // Key: t0, value: Event
    pub(crate) mappings: BTreeMap<T, T0>, // Key: t, value t0
    pub last_applied: T,                  // t of last applied entry
    pub(crate) latest_t0: T0,             // last created or recognized t0
    pub node_serial: u16,
    latest_hash: [u8; 32],
}

impl Store for MemStore {
    #[instrument(level = "trace")]
    fn new(node_serial: u16) -> Result<Self, SyneviError> {
        Ok(MemStore {
            events: BTreeMap::default(),
            mappings: BTreeMap::default(),
            last_applied: T::default(),
            latest_t0: T0::default(),
            node_serial,
            latest_hash: [0; 32],
        })
    }

    #[instrument(level = "trace")]
    fn init_t_zero(&mut self, node_serial: u16) -> T0 {
        let t0 = T0(self.latest_t0.next_with_node(node_serial).into_time());
        self.latest_t0 = t0;
        t0
    }

    #[instrument(level = "trace")]
    fn pre_accept_tx(
        &mut self,
        id: u128,
        t_zero: T0,
        transaction: Vec<u8>,
    ) -> Result<(T, HashSet<T0, RandomState>), SyneviError> {
        let (t, deps) = {
            let t = T(if let Some((last_t, _)) = self.mappings.last_key_value() {
                if **last_t > *t_zero {
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
        Ok((t, deps))
    }

    #[instrument(level = "trace")]
    fn get_tx_dependencies(&self, t: &T, t_zero: &T0) -> HashSet<T0, RandomState> {
        if &self.last_applied == t {
            return HashSet::default();
        }
        assert!(self.last_applied < *t);
        // What about deps with dep_t0 < last_applied_t0 && dep_t > t?
        let mut deps = HashSet::default();

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
    fn accept_tx_ballot(&mut self, t_zero: &T0, ballot: Ballot) -> Option<Ballot> {
        let event = self.events.get_mut(t_zero)?;

        if event.ballot < ballot {
            event.ballot = ballot;
        }

        Some(event.ballot)
    }

    #[instrument(level = "trace")]
    fn upsert_tx(&mut self, upsert_event: UpsertEvent) -> Result<Option<Hashes>, SyneviError> {
        let Some(event) = self.events.get_mut(&upsert_event.t_zero) else {
            let event = Event::from(upsert_event.clone());
            self.events.insert(upsert_event.t_zero, event);
            self.mappings.insert(upsert_event.t, upsert_event.t_zero);
            return Ok(None);
        };

        // Update the latest t0
        if self.latest_t0 < event.t_zero {
            self.latest_t0 = event.t_zero;
        }

        // Do not update to a "lower" state
        if upsert_event.state < event.state {
            return Ok(None);
        }

        // Event is already applied
        if event.state == State::Applied {
            return Ok(event.hashes.clone());
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
                self.last_applied = event.t;
                let hashes = event.hash_event(
                    upsert_event
                        .execution_hash
                        .ok_or_else(|| SyneviError::MissingExecutionHash)?,
                    self.latest_hash,
                );
                self.latest_hash = hashes.transaction_hash;
                event.hashes = Some(hashes);
            };

            Ok(event.hashes.clone())
        } else {
            Ok(None)
        }
    }

    #[instrument(level = "trace")]
    fn get_recover_deps(&self, t_zero: &T0) -> Result<RecoverDependencies, SyneviError> {
        let mut recover_deps = RecoverDependencies {
            timestamp: self
                .events
                .get(t_zero)
                .map(|event| event.t)
                .ok_or_else(|| SyneviError::EventNotFound(t_zero.get_inner()))?,
            ..Default::default()
        };
        for (t_dep, t_zero_dep) in self.mappings.range(self.last_applied..) {
            let dep_event = self
                .events
                .get(t_zero_dep)
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

    fn get_event_state(&self, t_zero: &T0) -> Option<State> {
        self.events.get(t_zero).map(|event| event.state)
    }

    fn recover_event(
        &mut self,
        t_zero_recover: &T0,
        node_serial: u16,
    ) -> Result<RecoverEvent, SyneviError> {
        let Some(state) = self.get_event_state(t_zero_recover) else {
            return Err(SyneviError::EventNotFound(t_zero_recover.get_inner()));
        };
        if matches!(state, synevi_types::State::Undefined) {
            return Err(SyneviError::UndefinedRecovery);
        }

        if let Some(event) = self.events.get_mut(t_zero_recover) {
            event.ballot = Ballot(event.ballot.next_with_node(node_serial).into_time());

            Ok(RecoverEvent {
                id: event.id,
                t_zero: event.t_zero,
                t: event.t,
                state,
                transaction: event.transaction.clone(),
                dependencies: event.dependencies.clone(),
                ballot: event.ballot,
            })
        } else {
            Err(SyneviError::EventNotFound(t_zero_recover.get_inner()))
        }
    }

    fn get_event_store(&self) -> BTreeMap<T0, Event> {
        self.events.clone()
    }
}