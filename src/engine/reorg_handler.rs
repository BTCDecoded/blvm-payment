//! Payment reorg handling.
//!
//! The module runtime delivers `BlockDisconnected`. This process does not subscribe
//! on the node event bus. `ChainReorg` is a summary; rollback follows each disconnected block.
//! Rebroadcast uses the tx cache, then `get_transaction`, then `submit_mempool_transaction`.

#[cfg(feature = "ctv")]
use crate::engine::chain::ChainView;
#[cfg(feature = "ctv")]
use crate::engine::settlement::SettlementMonitor;
#[cfg(feature = "ctv")]
use crate::engine::state_machine::{PaymentState, PaymentStateMachine};
#[cfg(feature = "ctv")]
use crate::engine::tx_cache::PaymentTxCache;
#[cfg(feature = "ctv")]
use tracing::{debug, info, warn};

/// Handles a disconnected block for payment settlement state.
#[cfg(feature = "ctv")]
pub struct PaymentReorgHandler;

#[cfg(feature = "ctv")]
impl PaymentReorgHandler {
    pub async fn on_block_disconnected(
        state_machine: &PaymentStateMachine,
        chain: Option<&dyn ChainView>,
        tx_cache: Option<&PaymentTxCache>,
        settlement_monitor: Option<&SettlementMonitor>,
        block_hash: &blvm_node::Hash,
        _height: u64,
    ) {
        let states = state_machine.list_payment_states();
        let affected: Vec<_> = states
            .into_iter()
            .filter_map(|(_id, state)| {
                if let PaymentState::Settled {
                    request_id,
                    tx_hash,
                    block_hash: settled_block,
                    expected_outputs,
                    ..
                } = state
                {
                    if &settled_block == block_hash {
                        Some((request_id, tx_hash, expected_outputs))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        let affected_count = affected.len();
        for (payment_id, tx_hash, expected_outputs) in affected {
            if let Some(chain) = chain {
                match chain.confirmed(&tx_hash).await {
                    Ok(Some(_)) => {
                        debug!(
                            "Reorg: payment {} tx {} still indexed, keeping Settled",
                            payment_id,
                            hex::encode(tx_hash)
                        );
                        continue;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(
                            "Reorg: payment {} tx metadata read failed: {}",
                            payment_id, error
                        );
                    }
                }
            }

            let in_mempool = match chain {
                Some(chain) => chain
                    .mempool_transaction(&tx_hash)
                    .await
                    .ok()
                    .flatten()
                    .is_some(),
                None => false,
            };
            if in_mempool {
                info!(
                    "Reorg: payment {} tx {} in mempool, downgrading to InMempool",
                    payment_id,
                    hex::encode(tx_hash)
                );
                let _ = state_machine.mark_in_mempool(&payment_id, tx_hash).await;
                Self::restart_monitoring_if_needed(
                    settlement_monitor,
                    &payment_id,
                    expected_outputs.as_deref(),
                    tx_hash,
                )
                .await;
                continue;
            }

            let cached = tx_cache.and_then(|cache| cache.get(&tx_hash));
            let tx_opt = match cached {
                Some(tx) => Some(tx),
                None => match chain {
                    Some(chain) => chain.get_transaction(&tx_hash).await.ok().flatten(),
                    None => None,
                },
            };

            if let (Some(chain), Some(tx)) = (chain, tx_opt) {
                match chain.submit(tx.clone()).await {
                    Ok(true) => {
                        if let Some(cache) = tx_cache {
                            cache.store(tx_hash, tx);
                        }
                        info!(
                            "Reorg: payment {} tx {} re-broadcast, downgrading to InMempool",
                            payment_id,
                            hex::encode(tx_hash)
                        );
                        let _ = state_machine.mark_in_mempool(&payment_id, tx_hash).await;
                        Self::restart_monitoring_if_needed(
                            settlement_monitor,
                            &payment_id,
                            expected_outputs.as_deref(),
                            tx_hash,
                        )
                        .await;
                        continue;
                    }
                    Ok(false) => {
                        debug!(
                            "Reorg: payment {} tx {} re-broadcast was already present",
                            payment_id,
                            hex::encode(tx_hash)
                        );
                    }
                    Err(error) => {
                        warn!(
                            "Reorg: payment {} tx {} re-broadcast failed: {}",
                            payment_id,
                            hex::encode(tx_hash),
                            error
                        );
                    }
                }
            }

            info!(
                "Reorg: payment {} tx {} not in mempool, marking ReorgPending",
                payment_id,
                hex::encode(tx_hash)
            );
            let _ = state_machine
                .mark_reorg_pending(
                    &payment_id,
                    tx_hash,
                    "Block disconnected, transaction not in mempool".to_string(),
                    expected_outputs,
                )
                .await;
        }

        if affected_count > 0 {
            debug!(
                "PaymentReorgHandler: processed {} affected payment(s) for block {:?}",
                affected_count, block_hash
            );
        }
    }

    async fn restart_monitoring_if_needed(
        settlement_monitor: Option<&SettlementMonitor>,
        payment_id: &str,
        expected_outputs: Option<&[blvm_protocol::payment::PaymentOutput]>,
        tx_hash: blvm_node::Hash,
    ) {
        if let (Some(monitor), Some(outputs)) = (settlement_monitor, expected_outputs) {
            if !outputs.is_empty() {
                if let Err(error) = monitor
                    .start_monitoring(payment_id, outputs.to_vec(), Some(tx_hash))
                    .await
                {
                    warn!(
                        "Reorg: failed to restart monitoring for payment {}: {}",
                        payment_id, error
                    );
                }
            }
        }
    }

    pub async fn recheck_reorg_pending(
        state_machine: &PaymentStateMachine,
        chain: Option<&dyn ChainView>,
        settlement_monitor: Option<&SettlementMonitor>,
    ) {
        let Some(chain) = chain else {
            return;
        };
        let states = state_machine.list_payment_states();
        for (_id, state) in states {
            let PaymentState::ReorgPending {
                request_id,
                tx_hash,
                expected_outputs,
                ..
            } = state
            else {
                continue;
            };
            if chain
                .mempool_transaction(&tx_hash)
                .await
                .ok()
                .flatten()
                .is_some()
            {
                info!(
                    "ReorgPending re-check: payment {} tx {} now in mempool",
                    request_id,
                    hex::encode(tx_hash)
                );
                let _ = state_machine.mark_in_mempool(&request_id, tx_hash).await;
                Self::restart_monitoring_if_needed(
                    settlement_monitor,
                    &request_id,
                    expected_outputs.as_deref(),
                    tx_hash,
                )
                .await;
            }
        }
    }
}

#[cfg(not(feature = "ctv"))]
pub struct PaymentReorgHandler;
