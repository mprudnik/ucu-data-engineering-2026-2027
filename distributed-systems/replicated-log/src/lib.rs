//! Replicated log: one master and any number of secondaries.
//!
//! The master appends messages to its log and replicates them to the secondaries.
//! See [`core`] for the logic and [`http`] for the HTTP layer.

pub mod core;
pub mod http;
