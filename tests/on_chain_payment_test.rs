//! verify_on_chain_payment RPC round-trip (path 3, no CTV required).

#![cfg(feature = "bip70-http")]

use blvm_node::config::PaymentConfig;
use blvm_payment::engine::processor::{PaymentError, PaymentProcessor};
use blvm_payment::engine::rpc::{PaymentRpc, PaymentTxLookup};
use blvm_payment::engine::state_machine::{PaymentState, PaymentStateMachine};
use blvm_node::storage::hashing::hash_to_rpc_hex;
use blvm_node::{Hash, Transaction};
use blvm_protocol::block::calculate_tx_id;
use blvm_protocol::payment::PaymentOutput;
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

mod common;
use common::valid_transaction;

struct SeenTx(Transaction);

#[async_trait::async_trait]
impl PaymentTxLookup for SeenTx {
    async fn find_tx(
        &self,
        hash: &Hash,
    ) -> Result<Option<(Transaction, bool)>, PaymentError> {
        if calculate_tx_id(&self.0) == *hash {
            Ok(Some((self.0.clone(), true)))
        } else {
            Ok(None)
        }
    }
}

fn create_test_outputs() -> Vec<PaymentOutput> {
    vec![
        PaymentOutput {
            script: vec![0x51, 0x87],
            amount: Some(100_000),
        },
        PaymentOutput {
            script: vec![0x52, 0x87],
            amount: Some(50_000),
        },
    ]
}

fn create_rpc() -> (PaymentRpc, Arc<PaymentStateMachine>) {
    let processor =
        Arc::new(PaymentProcessor::new(PaymentConfig::default()).expect("payment processor"));
    let state_machine = Arc::new(PaymentStateMachine::new(processor));
    (
        PaymentRpc::with_state_machine(state_machine.clone()),
        state_machine,
    )
}

#[tokio::test]
async fn verify_on_chain_payment_in_mempool() {
    let (rpc, state_machine) = create_rpc();
    let (payment_id, _) = state_machine
        .create_payment_request(create_test_outputs(), None, false)
        .await
        .expect("create payment request");

    let tx_hash = [0x42u8; 32];
    state_machine
        .mark_in_mempool(&payment_id, tx_hash)
        .await
        .expect("mark in mempool");

    let result = rpc
        .verify_on_chain_payment(&json!([payment_id, hex::encode(tx_hash)]))
        .await
        .expect("verify_on_chain_payment");

    assert_eq!(result["verified"], true);
    assert_eq!(result["state"], "in_mempool");
    assert_eq!(result["amount_sats"], 150_000);
    assert_eq!(result["tx_hash"], hex::encode(tx_hash));
}

#[tokio::test]
async fn verify_on_chain_payment_settled() {
    let (rpc, state_machine) = create_rpc();
    let (payment_id, _) = state_machine
        .create_payment_request(create_test_outputs(), None, false)
        .await
        .expect("create payment request");

    let tx_hash = [0x43u8; 32];
    let block_hash = [0x44u8; 32];
    state_machine
        .mark_in_mempool(&payment_id, tx_hash)
        .await
        .expect("mark in mempool");
    state_machine
        .mark_settled(&payment_id, tx_hash, block_hash, 6, None)
        .await
        .expect("mark settled");

    let result = rpc
        .verify_on_chain_payment(&json!([payment_id, hex::encode(tx_hash)]))
        .await
        .expect("verify_on_chain_payment");

    assert_eq!(result["verified"], true);
    assert_eq!(result["state"], "settled");
    assert_eq!(result["amount_sats"], 150_000);
}

#[tokio::test]
async fn verify_on_chain_payment_tx_mismatch() {
    let (rpc, state_machine) = create_rpc();
    let (payment_id, _) = state_machine
        .create_payment_request(create_test_outputs(), None, false)
        .await
        .expect("create payment request");

    let tx_hash = [0x45u8; 32];
    state_machine
        .mark_in_mempool(&payment_id, tx_hash)
        .await
        .expect("mark in mempool");

    let wrong_hash = [0x99u8; 32];
    let result = rpc
        .verify_on_chain_payment(&json!([payment_id, hex::encode(wrong_hash)]))
        .await
        .expect("verify_on_chain_payment");

    assert_eq!(result["verified"], false);
    assert_eq!(result["state"], "tx_mismatch");
}

#[tokio::test]
async fn verify_on_chain_payment_pending() {
    let (rpc, state_machine) = create_rpc();
    let (payment_id, _) = state_machine
        .create_payment_request(create_test_outputs(), None, false)
        .await
        .expect("create payment request");

    let state = state_machine
        .get_payment_state(&payment_id)
        .await
        .expect("get state");
    assert!(matches!(state, PaymentState::RequestCreated { .. }));

    let tx_hash = [0x46u8; 32];
    let result = rpc
        .verify_on_chain_payment(&json!([payment_id, hex::encode(tx_hash)]))
        .await
        .expect("verify_on_chain_payment");

    assert_eq!(result["verified"], false);
    assert_eq!(result["state"], "pending");
}

#[tokio::test]
async fn verify_on_chain_payment_not_found() {
    let (rpc, _) = create_rpc();

    let result = rpc
        .verify_on_chain_payment(&json!(["missing-id", hex::encode([0u8; 32])]))
        .await
        .expect("verify_on_chain_payment");

    assert_eq!(result["verified"], false);
    assert_eq!(result["state"], "not_found");
}

fn chain_rpc_fixture() -> (TempDir, PaymentRpc, [u8; 32]) {
    let temp_dir = TempDir::new().unwrap();
    let tx = valid_transaction();
    let tx_hash = calculate_tx_id(&tx);
    let processor =
        Arc::new(PaymentProcessor::new(PaymentConfig::default()).expect("payment processor"));
    let state_machine = Arc::new(PaymentStateMachine::new(processor));
    let rpc = PaymentRpc::with_state_machine(state_machine)
        .with_tx_lookup(Arc::new(SeenTx(tx)));
    (temp_dir, rpc, tx_hash)
}

#[tokio::test]
async fn verify_on_chain_payment_by_tx_chain_only() {
    let (_dir, rpc, tx_hash) = chain_rpc_fixture();
    let min_amount = 1u64;

    let result = rpc
        .verify_on_chain_payment_by_tx(&json!([
            "foreign-payment-id",
            hash_to_rpc_hex(&tx_hash),
            min_amount
        ]))
        .await
        .expect("verify_on_chain_payment_by_tx");

    assert_eq!(result["verified"], true);
    assert_eq!(result["state"], "in_mempool");
    assert!(result["amount_sats"].as_u64().unwrap() >= min_amount);
}

#[tokio::test]
async fn verify_on_chain_payment_by_tx_underpaid() {
    let (_dir, rpc, tx_hash) = chain_rpc_fixture();

    let result = rpc
        .verify_on_chain_payment_by_tx(&json!([
            "foreign-payment-id",
            hash_to_rpc_hex(&tx_hash),
            u64::MAX
        ]))
        .await
        .expect("verify_on_chain_payment_by_tx");

    assert_eq!(result["verified"], false);
    assert_eq!(result["state"], "underpaid");
}

#[tokio::test]
async fn verify_on_chain_payment_by_tx_local_pr_outputs() {
    let temp_dir = TempDir::new().unwrap();
    let processor =
        Arc::new(PaymentProcessor::new(PaymentConfig::default()).expect("payment processor"));
    let state_machine = Arc::new(PaymentStateMachine::new(processor.clone()));
    let (payment_id, _) = state_machine
        .create_payment_request(create_test_outputs(), None, false)
        .await
        .expect("create payment request");

    let tx = valid_transaction();
    let tx_hash = calculate_tx_id(&tx);
    let rpc = PaymentRpc::with_state_machine(state_machine).with_tx_lookup(Arc::new(SeenTx(tx)));

    // Local PR exists but state is RequestCreated — chain path uses BIP70 outputs.
    let result = rpc
        .verify_on_chain_payment_by_tx(&json!([payment_id, hash_to_rpc_hex(&tx_hash), 1u64]))
        .await
        .expect("verify_on_chain_payment_by_tx");

    // Fixture tx does not match PR outputs → output_mismatch (not silent pass).
    assert_eq!(result["verified"], false);
    assert_eq!(result["state"], "output_mismatch");
}

#[tokio::test]
async fn verify_on_chain_payment_by_tx_delegates_when_tracked() {
    let (rpc, state_machine) = create_rpc();
    let (payment_id, _) = state_machine
        .create_payment_request(create_test_outputs(), None, false)
        .await
        .expect("create payment request");

    let tx_hash = [0x42u8; 32];
    state_machine
        .mark_in_mempool(&payment_id, tx_hash)
        .await
        .expect("mark in mempool");

    let result = rpc
        .verify_on_chain_payment_by_tx(&json!([payment_id, hex::encode(tx_hash), 100_000u64]))
        .await
        .expect("verify_on_chain_payment_by_tx");

    assert_eq!(result["verified"], true);
    assert_eq!(result["state"], "in_mempool");
}
