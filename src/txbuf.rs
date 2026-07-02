//! Per-transaction row buffering.
//!
//! WAL carries changes from transactions that may later abort; only rows
//! whose transaction reaches a COMMIT record may be written to the lake.
//! M0 scope: top-level xids only (subtransactions are treated as their own
//! xid — TODO M2: parse subxact lists from commit records).

use std::collections::HashMap;

use crate::wal::heap::Row;

#[derive(Default)]
pub struct TxBuffer {
    open: HashMap<u32, Vec<Row>>,
}

impl TxBuffer {
    pub fn add(&mut self, xid: u32, row: Row) {
        self.open.entry(xid).or_default().push(row);
    }

    /// Transaction committed: hand back its rows for the sink.
    pub fn commit(&mut self, xid: u32) -> Vec<Row> {
        self.open.remove(&xid).unwrap_or_default()
    }

    /// Transaction aborted: its rows must never reach the lake.
    pub fn abort(&mut self, xid: u32) -> usize {
        self.open.remove(&xid).map(|r| r.len()).unwrap_or(0)
    }

    pub fn open_count(&self) -> usize {
        self.open.len()
    }
}
