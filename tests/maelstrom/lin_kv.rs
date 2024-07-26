use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use synevi_kv::error::KVError;
use synevi_kv::kv_store::KVStore;
use synevi_network::network::Network;

use crate::maelstrom_config::MaelstromConfig;
use crate::messages::{Body, MessageType};
use crate::protocol::MessageHandler;

mod maelstrom_config;
mod messages;
mod network;
mod protocol;

#[tokio::main]
pub async fn main() -> Result<()> {
    maelstrom_only_setup().await
    //maelstrom_kv_setup().await
}

pub async fn maelstrom_only_setup() -> Result<()> {
    let config = MaelstromConfig::new("dummy1".to_string(), vec![]).await;
    let replica = Arc::new(MaelstromConfig::new("dummy2".to_string(), vec![]).await);
    config.spawn_server(replica).await
}

pub async fn maelstrom_kv_setup() -> Result<()> {
    let mut handler = MessageHandler::new();

    if let Some(msg) = handler.next() {
        if let MessageType::Init {
            ref node_id,
            ref node_ids,
        } = msg.body.msg_type
        {
            let id: u32 = node_id.chars().last().unwrap().into();

            let mut parsed_nodes = Vec::new();
            for node in node_ids {
                if node == node_id {
                    continue;
                }
                let node_id: u32 = node.chars().last().unwrap().into();
                let host = format!("http://localhost:{}", 11000 + node_id);
                eprintln!("{host}");
                parsed_nodes.push((DieselUlid::generate(), node_id as u16, host));
            }
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 11000 + id)).unwrap();
            eprintln!("{socket_addr}");
            let network = Arc::new(synevi_network::network::NetworkConfig::new(socket_addr));

            let reply = msg.reply(Body {
                msg_type: MessageType::InitOk,
                ..Default::default()
            });
            handler.send(&reply)?;
            let mut kv_store = KVStore::init(
                DieselUlid::generate(),
                id as u16,
                network.clone(),
                parsed_nodes,
                None,
            )
            .await
            .unwrap();

            while let Some(msg) = handler.next() {
                eprintln!("{msg:?}");
                match msg.body.msg_type {
                    MessageType::Read { key } => {
                        match kv_store.read(key.to_string()).await {
                            Ok(value) => {
                                let reply = msg.reply(Body {
                                    msg_type: MessageType::ReadOk {
                                        value: value.parse()?,
                                    },
                                    ..Default::default()
                                });
                                handler.send(&reply)?;
                            }
                            Err(err) => {
                                let reply = msg.reply(Body {
                                    msg_type: MessageType::Error {
                                        code: 20,
                                        text: format!("{err}"),
                                    },
                                    ..Default::default()
                                });
                                handler.send(&reply)?;
                            }
                        };
                    }
                    MessageType::Write { ref key, ref value } => {
                        let reply = match kv_store.write(key.to_string(), value.to_string()).await {
                            Ok(_) => msg.reply(Body {
                                msg_type: MessageType::WriteOk,
                                ..Default::default()
                            }),
                            Err(err) => {
                                eprintln!("Error {err}");
                                msg.reply(Body {
                                    msg_type: MessageType::WriteOk,
                                    ..Default::default()
                                })
                            }
                        };

                        handler.send(&reply)?;
                    }
                    MessageType::Cas {
                        ref key,
                        ref from,
                        ref to,
                    } => {
                        let reply = match kv_store
                            .cas(key.to_string(), from.to_string(), to.to_string())
                            .await
                        {
                            Ok(_) => msg.reply(Body {
                                msg_type: MessageType::CasOk,
                                ..Default::default()
                            }),

                            Err(err) => match err {
                                KVError::KeyNotFound => msg.reply(Body {
                                    msg_type: MessageType::Error {
                                        code: 20,
                                        text: format!("{err}"),
                                    },
                                    ..Default::default()
                                }),
                                KVError::MismatchError => msg.reply(Body {
                                    msg_type: MessageType::Error {
                                        code: 22,
                                        text: format!("{err}"),
                                    },
                                    ..Default::default()
                                }),
                                _ => {
                                    eprintln!("Error: {err}");
                                    msg.reply(Body {
                                        msg_type: MessageType::CasOk,
                                        ..Default::default()
                                    })
                                }
                            },
                        };
                        handler.send(&reply)?;
                    }
                    msg => eprintln!("Unexpected msg type {msg:?}"),
                }
            }
        } else {
            eprintln!("Invalid message: {:?}", msg);
            return Err(anyhow!("Invalid message"));
        }
    } else {
        eprintln!("No message received");
        return Err(anyhow!("No message received"));
    }
    Ok(())
}
