//! Library surface: the whole transcoder is consumable as a crate — the
//! binary (`main.rs`) is a thin pgwire-driven embedder of `engine::Engine`,
//! and the v2a pageserver fork will be another. Examples/tests drive the
//! WAL machinery and layer-file readers directly.

pub mod catalog;
pub mod clog;
pub mod embed;
pub mod engine;
pub mod fragment;
pub mod multixact;
pub mod pgwire;
pub mod reconstruct;
pub mod schema;
pub mod serve;
pub mod sink;
pub mod snapshot;
pub mod txbuf;
pub mod wal;
