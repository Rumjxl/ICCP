//! Algorithm and flow management utilities
//!
//! This module provides high-level management interfaces for algorithms
//! and flow routing decisions.

use crate::{flow::FlowId, Result};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Flow key for algorithm selection
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct FlowKey {
    pub src_ip: u32,
    pub src_port: u16,
    pub dst_ip: u32,
    pub dst_port: u16,
}

impl FlowKey {
    pub fn new(src_ip: u32, src_port: u16, dst_ip: u32, dst_port: u16) -> Self {
        Self {
            src_ip,
            src_port,
            dst_ip,
            dst_port,
        }
    }

    pub fn from_datapath_info(info: &crate::algorithm::DatapathInfo) -> Self {
        Self::new(info.src_ip, info.src_port, info.dst_ip, info.dst_port)
    }
}

/// Algorithm selection rule
#[derive(Clone)]
pub enum AlgorithmRule {
    /// Use specific algorithm for exact flow match
    ExactMatch {
        flow_key: FlowKey,
        algorithm: String,
    },
    /// Use algorithm based on IP prefix
    IpPrefix {
        prefix: u32,
        mask: u32,
        algorithm: String,
    },
    /// Use algorithm based on port range
    PortRange {
        start_port: u16,
        end_port: u16,
        algorithm: String,
    },
    /// Use algorithm based on custom predicate
    Custom {
        predicate: Arc<dyn Fn(&FlowKey) -> bool + Send + Sync>,
        algorithm: String,
    },
    /// Default fallback algorithm
    Default { algorithm: String },
}

impl std::fmt::Debug for AlgorithmRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlgorithmRule::ExactMatch {
                flow_key,
                algorithm,
            } => f
                .debug_struct("ExactMatch")
                .field("flow_key", flow_key)
                .field("algorithm", algorithm)
                .finish(),
            AlgorithmRule::IpPrefix {
                prefix,
                mask,
                algorithm,
            } => f
                .debug_struct("IpPrefix")
                .field("prefix", prefix)
                .field("mask", mask)
                .field("algorithm", algorithm)
                .finish(),
            AlgorithmRule::PortRange {
                start_port,
                end_port,
                algorithm,
            } => f
                .debug_struct("PortRange")
                .field("start_port", start_port)
                .field("end_port", end_port)
                .field("algorithm", algorithm)
                .finish(),
            AlgorithmRule::Custom { algorithm, .. } => f
                .debug_struct("Custom")
                .field("algorithm", algorithm)
                .field("predicate", &"<fn>")
                .finish(),
            AlgorithmRule::Default { algorithm } => f
                .debug_struct("Default")
                .field("algorithm", algorithm)
                .finish(),
        }
    }
}

impl AlgorithmRule {
    pub fn matches(&self, flow_key: &FlowKey) -> bool {
        match self {
            AlgorithmRule::ExactMatch {
                flow_key: rule_key, ..
            } => rule_key == flow_key,
            AlgorithmRule::IpPrefix { prefix, mask, .. } => {
                (flow_key.src_ip & mask) == *prefix || (flow_key.dst_ip & mask) == *prefix
            }
            AlgorithmRule::PortRange {
                start_port,
                end_port,
                ..
            } => {
                (flow_key.src_port >= *start_port && flow_key.src_port <= *end_port)
                    || (flow_key.dst_port >= *start_port && flow_key.dst_port <= *end_port)
            }
            AlgorithmRule::Custom { predicate, .. } => predicate(flow_key),
            AlgorithmRule::Default { .. } => true,
        }
    }

    pub fn algorithm(&self) -> &str {
        match self {
            AlgorithmRule::ExactMatch { algorithm, .. }
            | AlgorithmRule::IpPrefix { algorithm, .. }
            | AlgorithmRule::PortRange { algorithm, .. }
            | AlgorithmRule::Custom { algorithm, .. }
            | AlgorithmRule::Default { algorithm } => algorithm,
        }
    }
}

/// High-performance algorithm manager with concurrent access
pub struct AlgorithmManager {
    /// Flow-specific algorithm mappings
    flow_mappings: DashMap<FlowKey, String>,
    /// Ordered list of algorithm selection rules
    rules: parking_lot::RwLock<Vec<AlgorithmRule>>,
    /// Default algorithm name
    default_algorithm: parking_lot::RwLock<String>,
    /// Statistics
    stats: AlgorithmManagerStats,
}

#[derive(Debug, Default)]
pub struct AlgorithmManagerStats {
    pub total_lookups: AtomicU64,
    pub cache_hits: AtomicU64,
    pub rule_matches: AtomicU64,
    pub default_fallbacks: AtomicU64,
}

impl AlgorithmManagerStats {
    pub fn get_cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed) as f64;
        let total = self.total_lookups.load(Ordering::Relaxed) as f64;
        if total > 0.0 {
            hits / total
        } else {
            0.0
        }
    }
}

impl AlgorithmManager {
    pub fn new() -> Self {
        Self {
            flow_mappings: DashMap::new(),
            rules: parking_lot::RwLock::new(Vec::new()),
            default_algorithm: parking_lot::RwLock::new("cubic".to_string()),
            stats: AlgorithmManagerStats::default(),
        }
    }

    /// Set the default algorithm
    pub fn set_default_algorithm(&self, algorithm: String) {
        let mut default = self.default_algorithm.write();
        *default = algorithm.clone();
        info!(algorithm = %algorithm, "Set default algorithm");
    }

    /// Get the currently configured default algorithm.
    pub fn default_algorithm(&self) -> String {
        self.default_algorithm.read().clone()
    }

    /// Add an exact flow mapping
    pub fn add_flow_mapping(&self, flow_key: FlowKey, algorithm: String) {
        debug!(
            src_ip = flow_key.src_ip,
            src_port = flow_key.src_port,
            dst_ip = flow_key.dst_ip,
            dst_port = flow_key.dst_port,
            algorithm = %algorithm,
            "Adding flow mapping"
        );

        self.flow_mappings.insert(flow_key, algorithm);
    }

    /// Add an algorithm selection rule
    pub fn add_rule(&self, rule: AlgorithmRule) {
        debug!(algorithm = %rule.algorithm(), "Adding algorithm rule");
        let mut rules = self.rules.write();
        rules.push(rule);
    }

    /// Remove a flow mapping
    pub fn remove_flow_mapping(&self, flow_key: &FlowKey) -> Option<String> {
        self.flow_mappings.remove(flow_key).map(|(_, v)| v)
    }

    /// Clear all rules
    pub fn clear_rules(&self) {
        let mut rules = self.rules.write();
        rules.clear();
        info!("Cleared all algorithm rules");
    }

    /// Get algorithm for a flow
    pub fn get_algorithm(&self, flow_key: &FlowKey) -> String {
        self.stats.total_lookups.fetch_add(1, Ordering::Relaxed);

        // First check exact flow mappings (cache)
        if let Some(algorithm) = self.flow_mappings.get(flow_key) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            debug!(
                src_ip = flow_key.src_ip,
                src_port = flow_key.src_port,
                dst_ip = flow_key.dst_ip,
                dst_port = flow_key.dst_port,
                algorithm = %algorithm.value(),
                "Found cached algorithm mapping"
            );
            return algorithm.value().clone();
        }

        // Check rules in order
        let rules = self.rules.read();
        for rule in rules.iter() {
            if rule.matches(flow_key) {
                let algorithm = rule.algorithm().to_string();
                self.stats.rule_matches.fetch_add(1, Ordering::Relaxed);

                debug!(
                    src_ip = flow_key.src_ip,
                    src_port = flow_key.src_port,
                    dst_ip = flow_key.dst_ip,
                    dst_port = flow_key.dst_port,
                    algorithm = %algorithm,
                    "Matched algorithm rule"
                );

                // Cache the result for future lookups
                self.flow_mappings
                    .insert(flow_key.clone(), algorithm.clone());
                return algorithm;
            }
        }

        // Fall back to default
        self.stats.default_fallbacks.fetch_add(1, Ordering::Relaxed);
        let default = self.default_algorithm.read().clone();

        debug!(
            src_ip = flow_key.src_ip,
            src_port = flow_key.src_port,
            dst_ip = flow_key.dst_ip,
            dst_port = flow_key.dst_port,
            algorithm = %default,
            "Using default algorithm"
        );

        // Cache the default result
        self.flow_mappings.insert(flow_key.clone(), default.clone());
        default
    }

    /// Get statistics
    pub fn get_stats(&self) -> AlgorithmManagerStats {
        AlgorithmManagerStats {
            total_lookups: AtomicU64::new(self.stats.total_lookups.load(Ordering::Relaxed)),
            cache_hits: AtomicU64::new(self.stats.cache_hits.load(Ordering::Relaxed)),
            rule_matches: AtomicU64::new(self.stats.rule_matches.load(Ordering::Relaxed)),
            default_fallbacks: AtomicU64::new(self.stats.default_fallbacks.load(Ordering::Relaxed)),
        }
    }

    /// Get current flow count
    pub fn flow_count(&self) -> usize {
        self.flow_mappings.len()
    }

    /// List all active flow mappings
    pub fn list_flows(&self) -> Vec<(FlowKey, String)> {
        self.flow_mappings
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Clean up old flow mappings (useful for long-running systems)
    pub fn cleanup_flows(&self, keep_recent: usize) -> usize {
        let current_count = self.flow_mappings.len();
        if current_count <= keep_recent {
            return 0;
        }

        let to_remove = current_count - keep_recent;
        let mut removed = 0;

        // Remove oldest entries (this is a simple implementation)
        // In a real system, you might want to track access times
        let keys_to_remove: Vec<FlowKey> = self
            .flow_mappings
            .iter()
            .take(to_remove)
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            if self.flow_mappings.remove(&key).is_some() {
                removed += 1;
            }
        }

        if removed > 0 {
            info!(removed = removed, "Cleaned up old flow mappings");
        }

        removed
    }
}

/// Builder for creating algorithm rules
pub struct RuleBuilder;

impl RuleBuilder {
    /// Create an exact match rule
    pub fn exact_match(flow_key: FlowKey, algorithm: String) -> AlgorithmRule {
        AlgorithmRule::ExactMatch {
            flow_key,
            algorithm,
        }
    }

    /// Create an IP prefix rule
    pub fn ip_prefix(prefix: u32, mask: u32, algorithm: String) -> AlgorithmRule {
        AlgorithmRule::IpPrefix {
            prefix,
            mask,
            algorithm,
        }
    }

    /// Create a port range rule
    pub fn port_range(start_port: u16, end_port: u16, algorithm: String) -> AlgorithmRule {
        AlgorithmRule::PortRange {
            start_port,
            end_port,
            algorithm,
        }
    }

    /// Create a custom rule with a predicate function
    pub fn custom<F>(predicate: F, algorithm: String) -> AlgorithmRule
    where
        F: Fn(&FlowKey) -> bool + Send + Sync + 'static,
    {
        AlgorithmRule::Custom {
            predicate: Arc::new(predicate),
            algorithm,
        }
    }

    /// Create a default rule
    pub fn default(algorithm: String) -> AlgorithmRule {
        AlgorithmRule::Default { algorithm }
    }

    /// Create a rule for HTTP traffic (port 80 or 8080)
    pub fn http_traffic(algorithm: String) -> AlgorithmRule {
        Self::custom(
            |flow_key| {
                flow_key.src_port == 80
                    || flow_key.dst_port == 80
                    || flow_key.src_port == 8080
                    || flow_key.dst_port == 8080
            },
            algorithm,
        )
    }

    /// Create a rule for HTTPS traffic (port 443)
    pub fn https_traffic(algorithm: String) -> AlgorithmRule {
        Self::custom(
            |flow_key| flow_key.src_port == 443 || flow_key.dst_port == 443,
            algorithm,
        )
    }

    /// Create a rule for local traffic (127.0.0.0/8)
    pub fn local_traffic(algorithm: String) -> AlgorithmRule {
        Self::ip_prefix(0x7F000000, 0xFF000000, algorithm) // 127.0.0.0/8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_algorithm_manager() {
        let manager = AlgorithmManager::new();

        // Set default
        manager.set_default_algorithm("cubic".to_string());

        // Add specific mapping
        let flow_key = FlowKey::new(0x01020304, 8080, 0x05060708, 80);
        manager.add_flow_mapping(flow_key.clone(), "bbr".to_string());

        // Test lookup
        assert_eq!(manager.get_algorithm(&flow_key), "bbr");

        // Test default fallback
        let other_flow = FlowKey::new(0x09090909, 9090, 0x0A0A0A0A, 90);
        assert_eq!(manager.get_algorithm(&other_flow), "cubic");
    }

    #[test]
    fn test_algorithm_rules() {
        let manager = AlgorithmManager::new();
        manager.set_default_algorithm("cubic".to_string());

        // Add HTTP rule
        manager.add_rule(RuleBuilder::http_traffic("bbr".to_string()));

        // Test HTTP traffic
        let http_flow = FlowKey::new(0x01020304, 8080, 0x05060708, 80);
        assert_eq!(manager.get_algorithm(&http_flow), "bbr");

        // Test non-HTTP traffic
        let other_flow = FlowKey::new(0x01020304, 9090, 0x05060708, 9090);
        assert_eq!(manager.get_algorithm(&other_flow), "cubic");
    }

    #[test]
    fn test_ip_prefix_rule() {
        let manager = AlgorithmManager::new();
        manager.set_default_algorithm("cubic".to_string());

        // Add local traffic rule
        manager.add_rule(RuleBuilder::local_traffic("reno".to_string()));

        // Test local traffic
        let local_flow = FlowKey::new(0x7F000001, 8080, 0x05060708, 80); // 127.0.0.1
        assert_eq!(manager.get_algorithm(&local_flow), "reno");

        // Test non-local traffic
        let remote_flow = FlowKey::new(0x01020304, 8080, 0x05060708, 80);
        assert_eq!(manager.get_algorithm(&remote_flow), "cubic");
    }

    #[test]
    fn test_stats() {
        let manager = AlgorithmManager::new();
        manager.set_default_algorithm("cubic".to_string());

        let flow_key = FlowKey::new(0x01020304, 8080, 0x05060708, 80);

        // First lookup (miss)
        manager.get_algorithm(&flow_key);

        // Second lookup (hit)
        manager.get_algorithm(&flow_key);

        let stats = manager.get_stats();
        assert_eq!(stats.total_lookups.load(Ordering::Relaxed), 2);
        assert_eq!(stats.cache_hits.load(Ordering::Relaxed), 1);
        assert_eq!(stats.get_cache_hit_rate(), 0.5);
    }
}
