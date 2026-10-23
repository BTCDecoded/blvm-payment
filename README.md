# blvm-payment

Payment requests, vaults, and pools for `blvm`. This process keeps that state in memory. It does not open the node chain store.

The default `blvm` binary does not link these RPC methods. The node config pin is `blvm-payment = "0.1.*"`.

## Build

```bash
cd blvm-payment && cargo build
```

`[patch.crates-io]` points at sibling crates. CI strips that section.
