//! Payment requests for a loaded module.
//!
//! The default `blvm` binary does not register these methods. This process holds the
//! payment state machine in memory. `verifyonchainpaymentbytx` reads through the node
//! API. It does not open the node chain store.

use std::sync::Arc;

use blvm_node::config::PaymentConfig;
use blvm_node::module::traits::{ModuleError, NodeAPI};
use blvm_node::payment::processor::{PaymentError, PaymentProcessor};
use blvm_node::payment::state_machine::PaymentStateMachine;
use blvm_node::rpc::payment::{PaymentRpc, PaymentTxLookup};
use blvm_node::storage::database::Database;
use blvm_node::{Hash, Transaction};
use blvm_sdk::module::prelude::*;
use blvm_sdk_macros::module;
use serde_json::Value;

#[derive(Clone)]
pub struct PaymentModule {
    rpc: PaymentRpc,
}

impl PaymentModule {
    pub fn new() -> Result<Self, String> {
        let processor =
            PaymentProcessor::new(PaymentConfig::default()).map_err(|error| error.to_string())?;
        let rpc =
            PaymentRpc::with_state_machine(Arc::new(PaymentStateMachine::new(Arc::new(processor))));
        Ok(Self { rpc })
    }

    /// Chain reads for `verifyonchainpaymentbytx` go through the node API.
    pub fn with_node(self, api: Arc<dyn NodeAPI>) -> Self {
        Self {
            rpc: self.rpc.with_tx_lookup(Arc::new(NodePaymentLookup { api })),
        }
    }

    pub fn with_lookup(self, lookup: Arc<dyn PaymentTxLookup>) -> Self {
        Self {
            rpc: self.rpc.with_tx_lookup(lookup),
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

    #[command]
    fn help(&self, _ctx: &InvocationContext) -> Result<String, ModuleError> {
        Ok(
            "blvm-payment serves createpaymentrequest, getpaymentstate, listpayments, \
             verifyonchainpayment, verifyonchainpaymentbytx, createvault, getvaultstate, \
             createpool, and getpoolstate.\n\
             The default blvm node does not. Vault and pool state stay in this process. \
             Chain lookup is not attached."
                .into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blvm_node::payment::processor::PaymentError;
    use blvm_node::rpc::payment::PaymentTxLookup;
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
