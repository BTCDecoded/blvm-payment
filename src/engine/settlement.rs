//! Settlement Monitoring and Verification
//!
//! Monitors payment transactions from mempool through on-chain confirmation.
//! Tracks UTXO state, mempool transactions, and block confirmations to update
//! payment state machine.

use crate::engine::chain::ChainView;
use crate::engine::processor::PaymentError;
use crate::engine::state_machine::PaymentStateMachine;
use blvm_node::{Hash, Transaction};
use blvm_protocol::payment::PaymentOutput;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

/// Settlement monitor for payment transactions
pub struct SettlementMonitor {
    /// Payment state machine to update
    state_machine: Arc<PaymentStateMachine>,
    /// Chain and mempool reads. Absent until the module attaches a node.
    chain: Option<Arc<dyn ChainView>>,
    /// Tx cache for re-broadcast on reorg (optional)
    #[cfg(feature = "ctv")]
    tx_cache: Option<Arc<crate::engine::PaymentTxCache>>,
    /// Track payment outputs we're monitoring (payment_id -> outputs)
    monitored_payments: Arc<tokio::sync::RwLock<HashMap<String, Vec<PaymentOutput>>>>,
    /// Track expected transaction hashes (payment_id -> tx_hash)
    expected_transactions: Arc<tokio::sync::RwLock<HashMap<String, Hash>>>,
    /// Confirmations required before a payment is marked settled.
    safe_confirmation_depth: u32,
}

impl SettlementMonitor {
    /// Create a new settlement monitor
    pub fn new(state_machine: Arc<PaymentStateMachine>) -> Self {
        Self {
            state_machine,
            chain: None,
            #[cfg(feature = "ctv")]
            tx_cache: None,
            monitored_payments: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            expected_transactions: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            safe_confirmation_depth: 6,
        }
    }

    /// Confirmations required before `mark_settled`. Default 6.
    pub fn with_confirmation_depth(mut self, depth: u32) -> Self {
        self.safe_confirmation_depth = depth.max(1);
        self
    }

    /// Attach chain and mempool reads.
    pub fn with_chain(mut self, chain: Arc<dyn ChainView>) -> Self {
        self.chain = Some(chain);
        self
    }

    /// Set tx cache for re-broadcast on reorg
    #[cfg(feature = "ctv")]
    pub fn with_tx_cache(mut self, tx_cache: Arc<crate::engine::PaymentTxCache>) -> Self {
        self.tx_cache = Some(tx_cache);
        self
    }

    /// Start monitoring a payment for settlement
    ///
    /// # Arguments
    ///
    /// * `payment_id` - Payment request ID to monitor
    /// * `expected_outputs` - Payment outputs to match against transactions
    /// * `expected_tx_hash` - Optional expected transaction hash (if known)
    pub async fn start_monitoring(
        &self,
        payment_id: &str,
        expected_outputs: Vec<PaymentOutput>,
        expected_tx_hash: Option<Hash>,
    ) -> Result<(), PaymentError> {
        debug!("Starting settlement monitoring for payment: {}", payment_id);

        // Store monitored outputs
        {
            let mut monitored = self.monitored_payments.write().await;
            monitored.insert(payment_id.to_string(), expected_outputs);
        }

        // Store expected transaction hash if provided
        if let Some(tx_hash) = expected_tx_hash {
            let mut expected = self.expected_transactions.write().await;
            expected.insert(payment_id.to_string(), tx_hash);
        }

        // Start background monitoring task
        let monitor = self.clone_for_monitoring();
        let payment_id_clone = payment_id.to_string();
        tokio::spawn(async move {
            monitor.monitor_payment_settlement(&payment_id_clone).await;
        });

        Ok(())
    }

    /// Stop monitoring a payment
    pub async fn stop_monitoring(&self, payment_id: &str) {
        let mut monitored = self.monitored_payments.write().await;
        monitored.remove(payment_id);
        let mut expected = self.expected_transactions.write().await;
        expected.remove(payment_id);
    }

    /// Monitor payment settlement (background task)
    async fn monitor_payment_settlement(&self, payment_id: &str) {
        info!("Monitoring settlement for payment: {}", payment_id);

        // Get expected outputs
        let expected_outputs = {
            let monitored = self.monitored_payments.read().await;
            monitored.get(payment_id).cloned()
        };

        if expected_outputs.is_none() {
            warn!("No expected outputs found for payment: {}", payment_id);
            return;
        }

        let expected_outputs = expected_outputs.unwrap();
        let expected_tx_hash = {
            let expected = self.expected_transactions.read().await;
            expected.get(payment_id).copied()
        };

        // Monitor for up to 24 hours (86400 seconds)
        let max_duration = Duration::from_secs(86400);
        let start_time = std::time::Instant::now();
        let check_interval = Duration::from_secs(10); // Check every 10 seconds

        loop {
            // Check timeout
            if start_time.elapsed() > max_duration {
                warn!("Settlement monitoring timeout for payment: {}", payment_id);
                let _ = self
                    .state_machine
                    .mark_failed(
                        payment_id,
                        "Settlement monitoring timeout (24 hours)".to_string(),
                    )
                    .await;
                break;
            }

            // Check mempool first
            if let Some(chain) = &self.chain {
                if let Ok(tx_hash) = self
                    .check_mempool(chain.as_ref(), payment_id, &expected_outputs, expected_tx_hash)
                    .await
                {
                    // Transaction found in mempool
                    let _ = self
                        .state_machine
                        .mark_in_mempool(payment_id, tx_hash)
                        .await;
                }
            }

            // Check blockchain for confirmation
            if let Some(chain) = &self.chain {
                if let Ok(Some((tx_hash, block_hash, confirmations))) = self
                    .check_blockchain(chain.as_ref(), payment_id, &expected_outputs, expected_tx_hash)
                    .await
                {
                    if confirmations >= self.safe_confirmation_depth {
                        let _ = self
                            .state_machine
                            .mark_settled(
                                payment_id,
                                tx_hash,
                                block_hash,
                                confirmations,
                                Some(expected_outputs.to_vec()),
                            )
                            .await;
                        break;
                    }
                }
            }

            // Wait before next check
            sleep(check_interval).await;
        }

        // Clean up monitoring
        self.stop_monitoring(payment_id).await;
    }

    /// Check mempool for matching transaction
    async fn check_mempool(
        &self,
        chain: &dyn ChainView,
        payment_id: &str,
        expected_outputs: &[PaymentOutput],
        expected_tx_hash: Option<Hash>,
    ) -> Result<Hash, PaymentError> {
        if let Some(expected_hash) = expected_tx_hash {
            if let Some(tx) = chain.mempool_transaction(&expected_hash).await? {
                #[cfg(feature = "ctv")]
                if let Some(ref cache) = self.tx_cache {
                    cache.store(expected_hash, tx);
                }
                return Ok(expected_hash);
            }
        }

        for tx in chain.mempool_transactions().await? {
            if self.transaction_matches_outputs(&tx, expected_outputs) {
                let tx_hash = blvm_protocol::block::calculate_tx_id(&tx);
                #[cfg(feature = "ctv")]
                if let Some(ref cache) = self.tx_cache {
                    cache.store(tx_hash, tx.clone());
                }
                debug!(
                    "Found matching transaction in mempool for payment {}: {}",
                    payment_id,
                    hex::encode(tx_hash)
                );
                return Ok(tx_hash);
            }
        }

        Err(PaymentError::ProcessingError(
            "Transaction not found in mempool".to_string(),
        ))
    }

    /// Confirm a known txid. No script index scan: an unknown txid stays unconfirmed.
    async fn check_blockchain(
        &self,
        chain: &dyn ChainView,
        payment_id: &str,
        expected_outputs: &[PaymentOutput],
        expected_tx_hash: Option<Hash>,
    ) -> Result<Option<(Hash, Hash, u32)>, PaymentError> {
        let Some(expected_hash) = expected_tx_hash else {
            return Ok(None);
        };
        self.lookup_confirmed_transaction(chain, payment_id, expected_hash, expected_outputs)
            .await
    }

    async fn lookup_confirmed_transaction(
        &self,
        chain: &dyn ChainView,
        payment_id: &str,
        tx_hash: Hash,
        expected_outputs: &[PaymentOutput],
    ) -> Result<Option<(Hash, Hash, u32)>, PaymentError> {
        let Some((tx, block_hash, confirmations)) = chain.confirmed(&tx_hash).await? else {
            return Ok(None);
        };
        if !self.transaction_matches_outputs(&tx, expected_outputs) {
            return Ok(None);
        }
        #[cfg(feature = "ctv")]
        if let Some(ref cache) = self.tx_cache {
            cache.store(tx_hash, tx);
        }
        debug!(
            "Found confirmed transaction for payment {}: {} (block: {}, confirmations: {})",
            payment_id,
            hex::encode(tx_hash),
            hex::encode(block_hash),
            confirmations
        );
        Ok(Some((tx_hash, block_hash, confirmations)))
    }
    /// Check if transaction matches expected payment outputs
    fn transaction_matches_outputs(
        &self,
        tx: &Transaction,
        expected_outputs: &[PaymentOutput],
    ) -> bool {
        // Convert expected outputs to transaction outputs for comparison
        use blvm_protocol::types::{ByteString, Integer, TransactionOutput};
        let expected_tx_outputs: Vec<TransactionOutput> = expected_outputs
            .iter()
            .filter_map(|po| {
                // PaymentOutput.amount might be Option<u64> or u64, handle both
                let amount = po.amount?;
                Some(TransactionOutput {
                    value: Integer::from(amount as i64),
                    script_pubkey: ByteString::from(po.script.clone()),
                })
            })
            .collect();

        // Check if transaction has matching outputs
        if tx.outputs.len() < expected_tx_outputs.len() {
            return false;
        }

        // Each expected output consumes one transaction output.
        let mut used = vec![false; tx.outputs.len()];
        for expected_output in &expected_tx_outputs {
            let mut found = false;
            for (i, tx_output) in tx.outputs.iter().enumerate() {
                if used[i] {
                    continue;
                }
                if tx_output.value == expected_output.value
                    && tx_output.script_pubkey == expected_output.script_pubkey
                {
                    used[i] = true;
                    found = true;
                    break;
                }
            }
            if !found {
                return false;
            }
        }

        true
    }

    /// Clone for background monitoring task
    fn clone_for_monitoring(&self) -> Self {
        Self {
            state_machine: Arc::clone(&self.state_machine),
            chain: self.chain.as_ref().map(Arc::clone),
            #[cfg(feature = "ctv")]
            tx_cache: self.tx_cache.as_ref().map(Arc::clone),
            monitored_payments: Arc::clone(&self.monitored_payments),
            expected_transactions: Arc::clone(&self.expected_transactions),
            safe_confirmation_depth: self.safe_confirmation_depth,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::processor::PaymentProcessor;
    use blvm_node::config::PaymentConfig;
    use blvm_protocol::{OutPoint, TransactionInput, TransactionOutput};
    use std::sync::Arc;
    use std::sync::Mutex;

    fn sample_payment_tx(script: Vec<u8>, amount: u64) -> Transaction {
        Transaction {
            version: 2,
            inputs: vec![TransactionInput {
                prevout: OutPoint {
                    hash: [1u8; 32],
                    index: 0,
                },
                script_sig: vec![],
                sequence: 0xffffffff,
            }]
            .into(),
            outputs: vec![TransactionOutput {
                value: amount as i64,
                script_pubkey: script,
            }]
            .into(),
            lock_time: 0,
        }
    }

    #[test]
    fn transaction_matches_outputs_requires_all_scripts_and_amounts() {
        let processor =
            Arc::new(PaymentProcessor::new(PaymentConfig::default()).expect("payment processor"));
        let monitor = SettlementMonitor::new(Arc::new(PaymentStateMachine::new(processor)));

        let script_a = vec![0x51, 0x87];
        let script_b = vec![0x52, 0x88];
        let tx = Transaction {
            version: 2,
            inputs: blvm_protocol::tx_inputs![],
            outputs: blvm_protocol::tx_outputs![
                TransactionOutput {
                    value: 10_000,
                    script_pubkey: script_a.clone(),
                },
                TransactionOutput {
                    value: 20_000,
                    script_pubkey: script_b.clone(),
                },
            ],
            lock_time: 0,
        };

        let expected = vec![
            PaymentOutput {
                script: script_a,
                amount: Some(10_000),
            },
            PaymentOutput {
                script: script_b,
                amount: Some(20_000),
            },
        ];

        assert!(monitor.transaction_matches_outputs(&tx, &expected));
        assert!(!monitor.transaction_matches_outputs(
            &tx,
            &[PaymentOutput {
                script: vec![0x53],
                amount: Some(10_000),
            }]
        ));
    }

    struct FakeChain {
        tip: u64,
        confirmed_height: Mutex<Option<(Transaction, Hash, u64)>>,
    }

    #[async_trait::async_trait]
    impl ChainView for FakeChain {
        async fn tip_height(&self) -> Result<u64, PaymentError> {
            Ok(self.tip)
        }
        async fn mempool_transaction(
            &self,
            _txid: &Hash,
        ) -> Result<Option<Transaction>, PaymentError> {
            Ok(None)
        }
        async fn mempool_transactions(&self) -> Result<Vec<Transaction>, PaymentError> {
            Ok(Vec::new())
        }
        async fn confirmed(
            &self,
            txid: &Hash,
        ) -> Result<Option<(Transaction, Hash, u32)>, PaymentError> {
            let guard = self.confirmed_height.lock().expect("chain");
            let Some((tx, block, height)) = guard.as_ref() else {
                return Ok(None);
            };
            let id = blvm_protocol::block::calculate_tx_id(tx);
            if &id != txid {
                return Ok(None);
            }
            let confirmations = (self.tip.saturating_sub(*height) + 1) as u32;
            Ok(Some((tx.clone(), *block, confirmations)))
        }
        async fn get_transaction(
            &self,
            _txid: &Hash,
        ) -> Result<Option<Transaction>, PaymentError> {
            Ok(None)
        }
        async fn submit(&self, _tx: Transaction) -> Result<bool, PaymentError> {
            Ok(true)
        }
        async fn fee_summary(&self) -> Result<(usize, u64, u64), PaymentError> {
            Ok((0, 0, 0))
        }
    }

    #[tokio::test]
    async fn known_txid_five_below_tip_has_six_confirmations_and_unknown_stays_unconfirmed() {
        let script = vec![0x51, 0x87];
        let tx = sample_payment_tx(script.clone(), 50_000);
        let txid = blvm_protocol::block::calculate_tx_id(&tx);
        let block_hash = [0x42u8; 32];
        let chain = Arc::new(FakeChain {
            tip: 10,
            confirmed_height: Mutex::new(Some((tx.clone(), block_hash, 5))),
        });
        let processor =
            Arc::new(PaymentProcessor::new(PaymentConfig::default()).expect("payment processor"));
        let monitor = SettlementMonitor::new(Arc::new(PaymentStateMachine::new(processor)))
            .with_chain(chain);
        let outputs = vec![PaymentOutput {
            script,
            amount: Some(50_000),
        }];
        let found = monitor
            .check_blockchain(monitor.chain.as_ref().unwrap().as_ref(), "payment-index", &outputs, Some(txid))
            .await
            .expect("lookup")
            .expect("confirmed tx");
        assert_eq!(found.0, txid);
        assert_eq!(found.1, block_hash);
        assert_eq!(found.2, 6);

        let missing = monitor
            .check_blockchain(
                monitor.chain.as_ref().unwrap().as_ref(),
                "payment-index",
                &outputs,
                None,
            )
            .await
            .expect("unknown txid");
        assert!(missing.is_none());
    }
}
