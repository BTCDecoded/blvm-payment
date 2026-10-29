//! Payment requests for a loaded module.
//!
//! The default `blvm` binary does not register these methods. This process holds the
//! only payment state machine. Chain and mempool reads go through the node API.

pub mod engine;

use std::sync::Arc;

use blvm_node::config::PaymentConfig;
use blvm_node::module::ipc::protocol::{EventMessage, EventPayload};
use blvm_node::module::traits::{ModuleError, NodeAPI};
use blvm_node::storage::database::Database;
use blvm_node::{Hash, Transaction};
use blvm_sdk::module::prelude::*;
use blvm_sdk_macros::module;
use engine::chain::{ChainView, NodeChain};
use engine::p2p::{handle_get_payment_request, handle_payment};
use engine::processor::{PaymentError, PaymentProcessor};
use engine::reorg_handler::PaymentReorgHandler;
use engine::rpc::{PaymentRpc, PaymentTxLookup};
use engine::settlement::SettlementMonitor;
use engine::state_machine::PaymentStateMachine;
use serde_json::Value;

fn payment_rpc<Fut>(
    rpc: PaymentRpc,
    params: Value,
    call: impl FnOnce(PaymentRpc, Value) -> Fut + Send + 'static,
) -> Result<Value, ModuleError>
where
    Fut: std::future::Future<Output = Result<Value, PaymentError>> + Send + 'static,
{
    run_async(async move { call(rpc, params).await.map_err(|error| error.to_string()) })
}

#[cfg(feature = "ctv")]
use engine::congestion::BatchConfig;

#[derive(Clone)]
pub struct PaymentModule {
    rpc: PaymentRpc,
    processor: Arc<PaymentProcessor>,
    state_machine: Arc<PaymentStateMachine>,
    settlement: Arc<SettlementMonitor>,
    chain: Arc<std::sync::Mutex<Option<Arc<dyn ChainView>>>>,
}

impl PaymentModule {
    pub fn new() -> Result<Self, String> {
        Self::open_with(None, PaymentConfig::default())
    }

    /// Open vault, pool, and batch trees in the module database.
    pub fn open(db: Arc<dyn Database>) -> Result<Self, String> {
        Self::open_with(Some(db), PaymentConfig::default())
    }

    /// Same as `open`, using the module `config.toml` when it deserializes as `PaymentConfig`.
    pub fn open_configured(db: Arc<dyn Database>, config: PaymentConfig) -> Result<Self, String> {
        Self::open_with(Some(db), config)
    }

    fn open_with(db: Option<Arc<dyn Database>>, config: PaymentConfig) -> Result<Self, String> {
        let processor = Arc::new(PaymentProcessor::new(config).map_err(|error| error.to_string())?);
        #[cfg(feature = "ctv")]
        let state_machine = {
            let machine = PaymentStateMachine::with_storage(Arc::clone(&processor), db.clone())
                .with_congestion_manager(db, BatchConfig::default());
            Arc::new(machine)
        };
        #[cfg(not(feature = "ctv"))]
        let state_machine = {
            let _ = db;
            Arc::new(PaymentStateMachine::new(Arc::clone(&processor)))
        };
        let settlement = Arc::new(SettlementMonitor::new(Arc::clone(&state_machine)));
        let rpc = PaymentRpc::with_state_machine(Arc::clone(&state_machine));
        Ok(Self {
            rpc,
            processor,
            state_machine,
            settlement,
            chain: Arc::new(std::sync::Mutex::new(None)),
        })
    }

    /// Chain reads for verification, settlement, and fee snapshots go through the node API.
    pub fn with_node(self, api: Arc<dyn NodeAPI>) -> Self {
        let chain = Arc::new(NodeChain {
            api: Arc::clone(&api),
        });
        #[cfg(feature = "ctv")]
        self.state_machine.set_node_api(Arc::clone(&api));
        *self.chain.lock().expect("payment chain") = Some(Arc::clone(&chain) as Arc<dyn ChainView>);
        let settlement = Arc::new(
            SettlementMonitor::new(Arc::clone(&self.state_machine))
                .with_chain(Arc::clone(&chain) as Arc<dyn ChainView>),
        );
        Self {
            rpc: self
                .rpc
                .with_tx_lookup(Arc::new(NodePaymentLookup { api }))
                .with_chain(chain),
            settlement,
            ..self
        }
    }

    pub fn with_lookup(self, lookup: Arc<dyn PaymentTxLookup>) -> Self {
        Self {
            rpc: self.rpc.with_tx_lookup(lookup),
            ..self
        }
    }
}

struct NodePaymentLookup {
    api: Arc<dyn NodeAPI>,
}

#[async_trait::async_trait]
impl PaymentTxLookup for NodePaymentLookup {
    async fn find_tx(&self, hash: &Hash) -> Result<Option<(Transaction, bool)>, PaymentError> {
        match self.api.get_mempool_transaction(hash).await {
            Ok(Some(tx)) => return Ok(Some((tx, true))),
            Ok(None) => {}
            Err(error) if error.to_string().contains("Mempool manager not available") => {}
            Err(error) => {
                return Err(PaymentError::ProcessingError(error.to_string()));
            }
        }
        match self.api.get_transaction(hash).await {
            Ok(tx) => Ok(tx.map(|tx| (tx, false))),
            Err(error) => Err(PaymentError::ProcessingError(error.to_string())),
        }
    }

    async fn confirmations(&self, hash: &Hash) -> Result<Option<u32>, PaymentError> {
        let metadata = self
            .api
            .get_tx_metadata(hash)
            .await
            .map_err(|error| PaymentError::ProcessingError(error.to_string()))?;
        let Some(metadata) = metadata else {
            return Ok(None);
        };
        let tip = self
            .api
            .get_block_height()
            .await
            .map_err(|error| PaymentError::ProcessingError(error.to_string()))?;
        Ok(Some((tip.saturating_sub(metadata.block_height) + 1) as u32))
    }
}

impl Default for PaymentModule {
    fn default() -> Self {
        Self::new().expect("payment processor")
    }
}

#[module(name = "blvm-payment")]
impl PaymentModule {
    #[rpc_method(name = "createpaymentrequest")]
    fn createpaymentrequest(
        &self,
        params: &Value,
        _db: &Arc<dyn Database>,
    ) -> Result<Value, ModuleError> {
        let rpc = self.rpc.clone();
        let params = params.clone();
        run_async(async move {
            rpc.create_payment_request(&params)
                .await
                .map_err(|error| error.to_string())
        })
    }

    #[rpc_method(name = "getpaymentstate")]
    fn getpaymentstate(
        &self,
        params: &Value,
        _db: &Arc<dyn Database>,
    ) -> Result<Value, ModuleError> {
        let rpc = self.rpc.clone();
        let params = params.clone();
        run_async(async move {
            rpc.get_payment_state(&params)
                .await
                .map_err(|error| error.to_string())
        })
    }

    #[rpc_method(name = "listpayments")]
    fn listpayments(&self, params: &Value, _db: &Arc<dyn Database>) -> Result<Value, ModuleError> {
        let rpc = self.rpc.clone();
        let params = params.clone();
        run_async(async move {
            rpc.list_payments(&params)
                .await
                .map_err(|error| error.to_string())
        })
    }

    #[rpc_method(name = "verifyonchainpayment")]
    fn verifyonchainpayment(
        &self,
        params: &Value,
        _db: &Arc<dyn Database>,
    ) -> Result<Value, ModuleError> {
        let rpc = self.rpc.clone();
        let params = params.clone();
        run_async(async move {
            rpc.verify_on_chain_payment(&params)
                .await
                .map_err(|error| error.to_string())
        })
    }

    #[rpc_method(name = "createvault")]
    fn createvault(&self, params: &Value, _db: &Arc<dyn Database>) -> Result<Value, ModuleError> {
        let rpc = self.rpc.clone();
        let params = params.clone();
        run_async(async move {
            rpc.create_vault(&params)
                .await
                .map_err(|error| error.to_string())
        })
    }

    #[rpc_method(name = "getvaultstate")]
    fn getvaultstate(&self, params: &Value, _db: &Arc<dyn Database>) -> Result<Value, ModuleError> {
        let rpc = self.rpc.clone();
        let params = params.clone();
        run_async(async move {
            rpc.get_vault_state(&params)
                .await
                .map_err(|error| error.to_string())
        })
    }

    #[rpc_method(name = "createpool")]
    fn createpool(&self, params: &Value, _db: &Arc<dyn Database>) -> Result<Value, ModuleError> {
        let rpc = self.rpc.clone();
        let params = params.clone();
        run_async(async move {
            rpc.create_pool(&params)
                .await
                .map_err(|error| error.to_string())
        })
    }

    #[rpc_method(name = "getpoolstate")]
    fn getpoolstate(&self, params: &Value, _db: &Arc<dyn Database>) -> Result<Value, ModuleError> {
        let rpc = self.rpc.clone();
        let params = params.clone();
        run_async(async move {
            rpc.get_pool_state(&params)
                .await
                .map_err(|error| error.to_string())
        })
    }

    #[rpc_method(name = "verifyonchainpaymentbytx")]
    fn verifyonchainpaymentbytx(
        &self,
        params: &Value,
        _db: &Arc<dyn Database>,
    ) -> Result<Value, ModuleError> {
        let rpc = self.rpc.clone();
        let params = params.clone();
        run_async(async move {
            rpc.verify_on_chain_payment_by_tx(&params)
                .await
                .map_err(|error| error.to_string())
        })
    }

    #[rpc_method(name = "verifycovenantproof")]
    fn verifycovenantproof(
        &self,
        params: &Value,
        _db: &Arc<dyn Database>,
    ) -> Result<Value, ModuleError> {
        payment_rpc(self.rpc.clone(), params.clone(), |rpc, params| async move {
            rpc.verify_covenant_proof(&params).await
        })
    }

    #[cfg(feature = "ctv")]
    #[rpc_method(name = "createcovenantproof")]
    fn createcovenantproof(
        &self,
        params: &Value,
        _db: &Arc<dyn Database>,
    ) -> Result<Value, ModuleError> {
        payment_rpc(self.rpc.clone(), params.clone(), |rpc, params| async move {
            rpc.create_covenant_proof(&params).await
        })
    }

    #[cfg(feature = "ctv")]
    #[rpc_method(name = "unvault")]
    fn unvault(&self, params: &Value, _db: &Arc<dyn Database>) -> Result<Value, ModuleError> {
        payment_rpc(self.rpc.clone(), params.clone(), |rpc, params| async move {
            rpc.unvault(&params).await
        })
    }

    #[cfg(feature = "ctv")]
    #[rpc_method(name = "withdrawfromvault")]
    fn withdrawfromvault(
        &self,
        params: &Value,
        _db: &Arc<dyn Database>,
    ) -> Result<Value, ModuleError> {
        payment_rpc(self.rpc.clone(), params.clone(), |rpc, params| async move {
            rpc.withdraw_from_vault(&params).await
        })
    }

    #[cfg(feature = "ctv")]
    #[rpc_method(name = "joinpool")]
    fn joinpool(&self, params: &Value, _db: &Arc<dyn Database>) -> Result<Value, ModuleError> {
        payment_rpc(self.rpc.clone(), params.clone(), |rpc, params| async move {
            rpc.join_pool(&params).await
        })
    }

    #[cfg(feature = "ctv")]
    #[rpc_method(name = "distributepool")]
    fn distributepool(
        &self,
        params: &Value,
        _db: &Arc<dyn Database>,
    ) -> Result<Value, ModuleError> {
        payment_rpc(self.rpc.clone(), params.clone(), |rpc, params| async move {
            rpc.distribute_pool(&params).await
        })
    }

    #[cfg(feature = "ctv")]
    #[rpc_method(name = "createbatch")]
    fn createbatch(&self, params: &Value, _db: &Arc<dyn Database>) -> Result<Value, ModuleError> {
        payment_rpc(self.rpc.clone(), params.clone(), |rpc, params| async move {
            rpc.create_batch(&params).await
        })
    }

    #[cfg(feature = "ctv")]
    #[rpc_method(name = "addtobatch")]
    fn addtobatch(&self, params: &Value, _db: &Arc<dyn Database>) -> Result<Value, ModuleError> {
        payment_rpc(self.rpc.clone(), params.clone(), |rpc, params| async move {
            rpc.add_to_batch(&params).await
        })
    }

    #[cfg(feature = "ctv")]
    #[rpc_method(name = "getcongestion")]
    fn getcongestion(&self, params: &Value, _db: &Arc<dyn Database>) -> Result<Value, ModuleError> {
        payment_rpc(self.rpc.clone(), params.clone(), |rpc, params| async move {
            rpc.get_congestion(&params).await
        })
    }

    #[cfg(feature = "ctv")]
    #[rpc_method(name = "getcongestionmetrics")]
    fn getcongestionmetrics(
        &self,
        params: &Value,
        _db: &Arc<dyn Database>,
    ) -> Result<Value, ModuleError> {
        payment_rpc(self.rpc.clone(), params.clone(), |rpc, params| async move {
            rpc.get_congestion_metrics(&params).await
        })
    }

    #[cfg(feature = "ctv")]
    #[rpc_method(name = "broadcastbatch")]
    fn broadcastbatch(
        &self,
        params: &Value,
        _db: &Arc<dyn Database>,
    ) -> Result<Value, ModuleError> {
        payment_rpc(self.rpc.clone(), params.clone(), |rpc, params| async move {
            rpc.broadcast_batch(&params).await
        })
    }

    #[on_event(PeerPayment, BlockDisconnected, ChainReorg)]
    async fn on_payment_event(
        &self,
        event: &EventMessage,
        ctx: &InvocationContext,
    ) -> Result<(), ModuleError> {
        match &event.payload {
            EventPayload::BlockDisconnected { hash, height } => {
                let chain = self
                    .chain
                    .lock()
                    .expect("payment chain")
                    .as_ref()
                    .map(Arc::clone);
                PaymentReorgHandler::on_block_disconnected(
                    &self.state_machine,
                    chain.as_deref(),
                    None,
                    Some(self.settlement.as_ref()),
                    hash,
                    *height,
                )
                .await;
                Ok(())
            }
            EventPayload::ChainReorg { .. } => Ok(()),
            EventPayload::PeerPayment {
                payload, peer_addr, ..
            } => {
                let api = ctx.node_api().ok_or_else(|| {
                    ModuleError::OperationError("node API is not available".to_string())
                })?;
                let parsed = blvm_node::network::protocol::ProtocolParser::parse_message(payload)
                    .map_err(|error| ModuleError::OperationError(error.to_string()))?;
                use blvm_node::network::protocol::ProtocolMessage;
                let response = match parsed {
                    ProtocolMessage::GetPaymentRequest(message) => {
                        let reply =
                            handle_get_payment_request(&message, Some(Arc::clone(&self.processor)))
                                .await
                                .map_err(|error| ModuleError::OperationError(error.to_string()))?;
                        ProtocolMessage::PaymentRequest(reply)
                    }
                    ProtocolMessage::Payment(message) => {
                        let merchant_key = self.processor.merchant_signing_key();
                        let reply = handle_payment(
                            &message,
                            Some(Arc::clone(&self.processor)),
                            merchant_key.as_ref(),
                        )
                        .await
                        .map_err(|error| ModuleError::OperationError(error.to_string()))?;
                        ProtocolMessage::PaymentACK(reply)
                    }
                    _ => return Ok(()),
                };
                let bytes =
                    blvm_node::network::protocol::ProtocolParser::serialize_message(&response)
                        .map_err(|error| ModuleError::OperationError(error.to_string()))?;
                api.send_peer_transport_payload(peer_addr.clone(), bytes)
                    .await
            }
            _ => Ok(()),
        }
    }

    #[command]
    fn help(&self, _ctx: &InvocationContext) -> Result<String, ModuleError> {
        Ok(
            "blvm-payment serves createpaymentrequest, createcovenantproof, getpaymentstate, \
             listpayments, verifyonchainpayment, verifyonchainpaymentbytx, verifycovenantproof, \
             createvault, unvault, withdrawfromvault, getvaultstate, createpool, joinpool, \
             distributepool, getpoolstate, createbatch, addtobatch, getcongestion, \
             getcongestionmetrics, and broadcastbatch.\n\
             The default blvm node does not. Vault, pool, and batch state stay in this process."
                .into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::processor::PaymentError;
    use crate::engine::rpc::PaymentTxLookup;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn createpaymentrequest_returns_the_state_machine_id() {
        let module = PaymentModule::new().expect("module");
        let dir = tempfile::tempdir().expect("tempdir");
        let db = blvm_sdk::module::ModuleDb::open(dir.path())
            .expect("db")
            .as_db();
        let created = module
            .dispatch_rpc(
                "createpaymentrequest",
                &json!([[{"script": [0x51], "amount": 1000}]]),
                &db,
            )
            .expect("create");
        let payment_id = created["payment_id"]
            .as_str()
            .expect("payment_id")
            .to_string();
        assert!(!payment_id.is_empty());
        let state = module
            .dispatch_rpc("getpaymentstate", &json!([payment_id]), &db)
            .expect("state");
        assert_eq!(state["state"], "request_created");
        assert_eq!(state["request_id"], created["payment_id"]);

        let listed = module
            .dispatch_rpc("listpayments", &json!([]), &db)
            .expect("list");
        assert_eq!(listed["count"], 1);
        assert_eq!(listed["payments"][0]["payment_id"], payment_id);
        assert_eq!(listed["payments"][0]["state"], "request_created");

        let tx_hash = "11".repeat(32);
        let verified = module
            .dispatch_rpc("verifyonchainpayment", &json!([payment_id, tx_hash]), &db)
            .expect("verify");
        assert_eq!(verified["verified"], false);
        assert_eq!(verified["state"], "pending");

        let chain = module.dispatch_rpc(
            "verifyonchainpaymentbytx",
            &json!([payment_id, tx_hash, 1000]),
            &db,
        );
        let message = chain.expect_err("no chain store").to_string();
        assert!(message.contains("chain access not available"), "{message}");

        let looked_up = PaymentModule::new()
            .expect("module")
            .with_lookup(Arc::new(FixedTx(matching_tx())));
        let created = looked_up
            .dispatch_rpc(
                "createpaymentrequest",
                &json!([[{"script": [0x51], "amount": 1000}]]),
                &db,
            )
            .expect("create");
        let payment_id = created["payment_id"].as_str().expect("payment_id");
        let confirmed = looked_up
            .dispatch_rpc(
                "verifyonchainpaymentbytx",
                &json!([payment_id, tx_hash, 1000]),
                &db,
            )
            .expect("lookup");
        assert_eq!(confirmed["verified"], true);
        assert_eq!(confirmed["state"], "confirmed");
        assert_eq!(confirmed["amount_sats"], 1000);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn createvault_keeps_the_vault_in_the_module() {
        let module = PaymentModule::new().expect("module");
        let dir = tempfile::tempdir().expect("tempdir");
        let db = blvm_sdk::module::ModuleDb::open(dir.path())
            .expect("db")
            .as_db();
        let created = module
            .dispatch_rpc(
                "createvault",
                &json!({
                    "vault_id": "v1",
                    "deposit_amount": 1000,
                    "withdrawal_script": "51"
                }),
                &db,
            )
            .expect("create vault");
        assert_eq!(created["vault_id"], "v1");
        let state = module
            .dispatch_rpc("getvaultstate", &json!({"vault_id": "v1"}), &db)
            .expect("vault state");
        assert_eq!(state["vault_id"], "v1");
        let unvaulted = module
            .dispatch_rpc(
                "unvault",
                &json!({"vault_id": "v1", "unvault_script": "52"}),
                &db,
            )
            .expect("unvault");
        assert_eq!(unvaulted["vault_id"], "v1");
        let batch = module
            .dispatch_rpc("createbatch", &json!({"batch_id": "b1"}), &db)
            .expect("create batch");
        assert_eq!(batch["batch_id"], "b1");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn createpool_keeps_the_pool_in_the_module() {
        let module = PaymentModule::new().expect("module");
        let dir = tempfile::tempdir().expect("tempdir");
        let db = blvm_sdk::module::ModuleDb::open(dir.path())
            .expect("db")
            .as_db();
        let created = module
            .dispatch_rpc(
                "createpool",
                &json!({
                    "pool_id": "p1",
                    "initial_participants": [["alice", 1000, "51"]]
                }),
                &db,
            )
            .expect("create pool");
        assert_eq!(created["pool_id"], "p1");
        let state = module
            .dispatch_rpc("getpoolstate", &json!({"pool_id": "p1"}), &db)
            .expect("pool state");
        assert_eq!(state["pool_id"], "p1");
        assert_eq!(state["pool_state"]["total_balance"], 1000);
    }

    struct FixedTx(blvm_node::Transaction);

    #[async_trait::async_trait]
    impl PaymentTxLookup for FixedTx {
        async fn find_tx(
            &self,
            _hash: &blvm_node::Hash,
        ) -> Result<Option<(blvm_node::Transaction, bool)>, PaymentError> {
            Ok(Some((self.0.clone(), false)))
        }
    }

    fn matching_tx() -> blvm_node::Transaction {
        blvm_node::Transaction {
            version: 2,
            inputs: Vec::new().into(),
            outputs: vec![blvm_node::TransactionOutput {
                value: 1000,
                script_pubkey: vec![0x51],
            }]
            .into(),
            lock_time: 0,
        }
    }
}
