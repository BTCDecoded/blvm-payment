//! Chain and mempool reads for the payment process.
//!
//! The module does not open the node chain store. Confirmations use tx-index
//! metadata. A payment with no stored txid is not confirmed by a script scan.

use crate::engine::processor::PaymentError;
use blvm_node::module::traits::{MempoolFeeRateSummary, ModuleError, NodeAPI};
use blvm_node::{Hash, Transaction};
use std::sync::Arc;

#[async_trait::async_trait]
pub trait ChainView: Send + Sync {
    async fn tip_height(&self) -> Result<u64, PaymentError>;
    async fn mempool_transaction(&self, txid: &Hash) -> Result<Option<Transaction>, PaymentError>;
    async fn mempool_transactions(&self) -> Result<Vec<Transaction>, PaymentError>;
    /// Confirmed body, block hash, and confirmation count. `None` if the txid is not indexed.
    async fn confirmed(
        &self,
        txid: &Hash,
    ) -> Result<Option<(Transaction, Hash, u32)>, PaymentError>;
    async fn get_transaction(&self, txid: &Hash) -> Result<Option<Transaction>, PaymentError>;
    async fn submit(&self, tx: Transaction) -> Result<bool, PaymentError>;
    async fn fee_summary(&self) -> Result<(usize, u64, u64), PaymentError>;
}

pub struct NodeChain {
    pub api: Arc<dyn NodeAPI>,
}

fn map_err(error: ModuleError) -> PaymentError {
    PaymentError::ProcessingError(error.to_string())
}

#[async_trait::async_trait]
impl ChainView for NodeChain {
    async fn tip_height(&self) -> Result<u64, PaymentError> {
        self.api.get_block_height().await.map_err(map_err)
    }

    async fn mempool_transaction(&self, txid: &Hash) -> Result<Option<Transaction>, PaymentError> {
        match self.api.get_mempool_transaction(txid).await {
            Ok(tx) => Ok(tx),
            Err(error) if error.to_string().contains("Mempool manager not available") => Ok(None),
            Err(error) => Err(map_err(error)),
        }
    }

    async fn mempool_transactions(&self) -> Result<Vec<Transaction>, PaymentError> {
        let hashes = match self.api.get_mempool_transactions().await {
            Ok(hashes) => hashes,
            Err(error) if error.to_string().contains("Mempool manager not available") => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(map_err(error)),
        };
        let mut txs = Vec::with_capacity(hashes.len());
        for hash in hashes {
            if let Some(tx) = self.mempool_transaction(&hash).await? {
                txs.push(tx);
            }
        }
        Ok(txs)
    }

    async fn confirmed(
        &self,
        txid: &Hash,
    ) -> Result<Option<(Transaction, Hash, u32)>, PaymentError> {
        let Some(tx) = self.api.get_transaction(txid).await.map_err(map_err)? else {
            return Ok(None);
        };
        let Some(metadata) = self.api.get_tx_metadata(txid).await.map_err(map_err)? else {
            return Ok(None);
        };
        let tip = self.api.get_block_height().await.map_err(map_err)?;
        let confirmations = (tip.saturating_sub(metadata.block_height) + 1) as u32;
        Ok(Some((tx, metadata.block_hash, confirmations)))
    }

    async fn get_transaction(&self, txid: &Hash) -> Result<Option<Transaction>, PaymentError> {
        self.api.get_transaction(txid).await.map_err(map_err)
    }

    async fn submit(&self, tx: Transaction) -> Result<bool, PaymentError> {
        self.api
            .submit_mempool_transaction(tx, None)
            .await
            .map_err(map_err)
    }

    async fn fee_summary(&self) -> Result<(usize, u64, u64), PaymentError> {
        let size = self
            .api
            .get_mempool_size()
            .await
            .map_err(map_err)?
            .transaction_count;
        let summary: MempoolFeeRateSummary = self
            .api
            .get_mempool_fee_rate_summary()
            .await
            .map_err(map_err)?;
        Ok((size, summary.avg_sat_vb, summary.median_sat_vb))
    }
}
