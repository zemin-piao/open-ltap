//! Library surface so examples/tests can drive the WAL machinery directly.
//! The binary (`main.rs`) declares the same modules; both build from one source.

pub mod pgwire;
pub mod schema;
pub mod txbuf;
pub mod wal;
