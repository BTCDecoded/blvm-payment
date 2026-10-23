//! blvm-payment — payment request module.
//!
//! The default node does not serve these methods. Load this process when a client needs them.

use anyhow::Result;
use blvm_payment::PaymentModule;
use blvm_sdk::module::{ModuleBootstrap, ModuleDb};
use std::sync::Arc;
use tracing::warn;

const MODULE_NAME: &str = "blvm-payment";

#[tokio::main]
async fn main() -> Result<()> {
    let bootstrap = ModuleBootstrap::init_module(MODULE_NAME);
    let db = ModuleDb::open_or_temp(&bootstrap.data_dir, MODULE_NAME)?;
    let module = PaymentModule::new().map_err(|error| anyhow::anyhow!(error))?;

    let setup = move |node_api: Arc<dyn blvm_node::module::traits::NodeAPI>,
                      _db: Arc<dyn blvm_node::storage::database::Database>,
                      _data_dir: &std::path::Path| {
        let module = module.clone().with_node(node_api);
        async move { Ok((module.clone(), module)) }
    };

    blvm_sdk::run_module! {
        bootstrap: &bootstrap,
        module_name: MODULE_NAME,
        module_type: PaymentModule,
        cli_type: PaymentModule,
        db: db.as_db(),
        setup: setup,
        event_types: PaymentModule::event_types(),
    }?;

    warn!("blvm-payment shutting down");
    Ok(())
}
