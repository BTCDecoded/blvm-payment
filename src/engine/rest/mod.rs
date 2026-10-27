//! Payment REST routes. The node REST server does not mount these.

#[cfg(feature = "bip70-http")]
pub mod payment;
#[cfg(all(feature = "bip70-http", feature = "ctv"))]
pub mod congestion;
#[cfg(all(feature = "bip70-http", feature = "ctv"))]
pub mod pool;
#[cfg(all(feature = "bip70-http", feature = "ctv"))]
pub mod vault;
