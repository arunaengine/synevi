use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::{Dependency, PreAcceptRequest, State};
use monotime::MonoTime;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::instrument;

use crate::utils::from_dependency;

static TIMEOUT: u64 = 10;
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
    events: RwLock<HashMap<MonoTime, Event>>, // Key: t0, value: Event
    mappings: RwLock<BTreeMap<MonoTime, MonoTime>>, // Key: t, value t0
    last_applied: RwLock<MonoTime>,           // t of last applied entry
}

#[derive(Clone, Debug)]
pub struct Event {
    pub t_zero: MonoTime,
    pub t: MonoTime, // maybe this should be changed to an actual timestamp
    pub state: State,
    pub event: Bytes,
    pub dependencies: HashMap<MonoTime, MonoTime>, // t and t_zero
    pub commit_notify: Arc<Notify>,
    pub apply_notify: Arc<Notify>,
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.t_zero == other.t_zero
            && self.t == other.t
            && self.state == other.state
            && self.event == other.event
            && self.dependencies == other.dependencies
    }
}

impl EventStore {
    #[instrument(level = "trace")]
    pub fn init() -> Self {
        EventStore {
            events: RwLock::new(HashMap::default()),
            mappings: RwLock::new(BTreeMap::default()),
            last_applied: RwLock::new(MonoTime::default()),
        }
    }

    #[instrument(level = "trace")]
    async fn insert(&self, event: Event) {
        self.events
            .write()
            .await
            .insert(event.t_zero, event.clone());
        self.mappings.write().await.insert(event.t, event.t_zero);
    }

    #[instrument(level = "trace")]
    pub async fn pre_accept(
        &self,
        request: PreAcceptRequest,
    ) -> Result<(Vec<Dependency>, MonoTime, MonoTime)> {
        // Parse t_zero
        let t_zero = MonoTime::try_from(request.timestamp_zero.as_slice())?;
        let (t, deps) = {
            // This lock is required that a t is uniquely decided
            let mut map_lock = self.mappings.write().await;
            let t = if let Some((last_t0, _)) = map_lock.last_key_value() {
                if last_t0 > &t_zero {
                    let t = t_zero.next_with_guard(last_t0).unwrap(); // This unwrap will not panic
                    t
                } else {
                    t_zero
                }
            } else {
                // No entries in the map -> insert the new event
                t_zero
            };
            map_lock.insert(t, t_zero);
            let last_applied = self.last_applied.read().await;
            // This might not be necessary to re-use the write lock here
            let deps: Vec<Dependency> = map_lock
                .range(*last_applied..t)
                .map(|(k, v)| Dependency {
                    timestamp: (*k).into(),
                    timestamp_zero: (*v).into(),
                })
                .collect();
            (t, deps)
        };

        let mut event_lock = self.events.write().await;

        let event = Event {
            t_zero,
            t,
            state: State::PreAccepted,
            event: request.event.into(),
            dependencies: from_dependency(deps.clone())?,
            commit_notify: Arc::new(Notify::new()),
            apply_notify: Arc::new(Notify::new()),
        };
        event_lock.insert(t_zero, event);
        Ok((deps, t_zero, t))
    }

    #[instrument(level = "trace")]
    pub async fn upsert(&self, event: Event) {
        let mut lock = self.events.write().await;
        //if let Some(old_event) = self.events.write().await.get_mut(&event.t_zero) {
        let old_event = lock.entry(event.t_zero).or_insert(event.clone());
        if old_event != &event {
            self.mappings.write().await.remove(&old_event.t);
            old_event.t = event.t;
            old_event.state = event.state;
            old_event.dependencies = event.dependencies;
            old_event.event = event.event;
            self.mappings.write().await.insert(event.t, event.t_zero);
            match event.state {
                State::Commited => old_event.commit_notify.notify_waiters(),
                State::Applied => {
                    let mut last = self.last_applied.write().await;
                    *last = event.t;
                    old_event.apply_notify.notify_waiters()
                }
                _ => {}
            }
        } else {
            match event.state {
                State::Commited => event.commit_notify.notify_waiters(),
                State::Applied => {
                    let mut last = self.last_applied.write().await;
                    *last = event.t;
                    event.apply_notify.notify_waiters()
                }
                _ => {}
            }
            // dbg!("[EVENT_STORE]: Insert");
            self.mappings.write().await.insert(event.t, event.t_zero);
            //self.insert(event).await;
            // dbg!("[EVENT_STORE]: Insert successful");
        };
    }

    #[instrument(level = "trace")]
    pub async fn get(&self, t_zero: &MonoTime) -> Option<Event> {
        self.events.read().await.get(t_zero).cloned()
    }

    #[instrument(level = "trace")]
    pub async fn last(&self) -> Option<Event> {
        if let Some((_, t_zero)) = self.mappings.read().await.last_key_value() {
            self.get(t_zero).await
        } else {
            None
        }
    }

    #[instrument(level = "trace")]
    pub async fn get_dependencies(&self, t: &MonoTime) -> Vec<Dependency> {
        let last = self.last_applied.read().await;
        if let Some(entry) = self.last().await {
            // Check if we can build a range over t -> If there is a newer timestamp than proposed t
            if &entry.t <= t {
                // ... if not, return [last..end]
                self.mappings
                    .read()
                    .await
                    .range(*last..)
                    .map(|(k, v)| Dependency {
                        timestamp: (*k).into(),
                        timestamp_zero: (*v).into(),
                    })
                    .collect()
            } else {
                // ... if yes, return [last..proposed_t]
                self.mappings
                    .read()
                    .await
                    .range(*last..*t)
                    .map(|(k, v)| Dependency {
                        timestamp: (*k).into(),
                        timestamp_zero: (*v).into(),
                    })
                    .collect()
            }
        } else {
            Vec::new()
        }
    }

    #[instrument(level = "trace")]
    pub async fn wait_for_dependencies(
        &self,
        dependencies: HashMap<MonoTime, MonoTime>,
        t_zero: MonoTime,
    ) -> Result<()> {
        // Collect notifies
        let mut notifies = JoinSet::new();
        for (dep_t, dep_t_zero) in dependencies.iter() {
            if dep_t_zero > &t_zero {
                let dep = self.events.read().await.get(dep_t_zero).cloned();
                if let Some(event) = dep {
                    if matches!(event.state, State::Commited | State::Applied) {
                        let notify = event.commit_notify.clone();
                        notifies.spawn(async move {
                            timeout(Duration::from_millis(TIMEOUT), notify.notified()).await
                        });
                    }
                } else {
                    let commit_notify = Arc::new(Notify::new());
                    let commit_clone = commit_notify.clone();
                    notifies.spawn(async move {
                        timeout(Duration::from_millis(TIMEOUT), commit_clone.notified()).await
                    });
                    self.insert(Event {
                        t_zero: *dep_t_zero,
                        t: *dep_t,
                        state: State::Undefined,
                        event: Default::default(),
                        dependencies: HashMap::default(),
                        commit_notify,
                        apply_notify: Arc::new(Notify::new()),
                    })
                    .await;
                }
            } else if let Some(event) = self.events.read().await.get(dep_t_zero) {
                if event.state != State::Applied {
                    let apply_clone = event.apply_notify.clone();
                    notifies.spawn(async move {
                        timeout(Duration::from_millis(TIMEOUT), apply_clone.notified()).await
                    });
                }
            } else {
                let apply_notify = Arc::new(Notify::new());
                let apply_clone = apply_notify.clone();
                notifies.spawn(async move {
                    timeout(Duration::from_millis(TIMEOUT), apply_clone.notified()).await
                });
                self.insert(Event {
                    t_zero: *dep_t_zero,
                    t: *dep_t,
                    state: State::Undefined,
                    event: Default::default(),
                    dependencies: HashMap::default(),
                    commit_notify: Arc::new(Notify::new()),
                    apply_notify,
                })
                .await;
            }
        }
        while let Some(x) = notifies.join_next().await {
            x??
            // TODO: Recovery when timeout
        }
        Ok(())
    }
}
