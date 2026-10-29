//! Shared fixtures for payment integration tests.

use blvm_node::storage::database::{Database, DatabaseBackend, create_database};
use blvm_node::{OutPoint, Transaction, TransactionInput, TransactionOutput};
use std::path::Path;
use std::sync::Arc;

pub fn module_db(dir: &Path) -> Arc<dyn Database> {
    Arc::from(create_database(dir, DatabaseBackend::Sled, None).expect("module database"))
}

pub fn test_secp256k1_scalar_one() -> [u8; 32] {
    let mut key = [0u8; 32];
    key[31] = 1;
    key
}

pub fn ecdsa_compact_sig_hex_and_pubkey_hex(
    seckey32: &[u8; 32],
    msg32: &[u8; 32],
) -> (String, String) {
    use blvm_secp256k1::ecdsa::{ecdsa_sign_compact_rfc6979, ge_to_compressed, pubkey_from_secret};
    use blvm_secp256k1::scalar::Scalar;
    let sig = ecdsa_sign_compact_rfc6979(msg32, seckey32).expect("ECDSA sign (test key)");
    let mut sec = Scalar::zero();
    assert!(
        !sec.set_b32(seckey32) && !sec.is_zero(),
        "invalid test seckey"
    );
    let pk = ge_to_compressed(&pubkey_from_secret(&sec));
    (hex::encode(sig), hex::encode(pk))
}

pub fn valid_transaction() -> Transaction {
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
            value: 1000,
            script_pubkey: vec![0x51],
        }]
        .into(),
        lock_time: 0,
    }
}
