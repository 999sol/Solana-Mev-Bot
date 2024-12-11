use dashmap::DashSet;

pub fn should_forward_tx(tx_cache: &DashSet<String>, tx_signature: &str) -> bool {
    // if the tx_cache does NOT contain the signature, we should forward it
    !tx_cache.contains(tx_signature)
}