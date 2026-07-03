//! Per-transaction row buffering.
//!
//! WAL carries changes from transactions that may later abort; only rows
//! whose transaction reaches a COMMIT record may be written to the lake.
//! Each open transaction remembers the LSN of its first change so a restart
//! can resume early enough to replay any transaction that was still in
//! flight (subtransactions: TODO M2 — parse subxact lists from commit records).

use std::collections::HashMap;

use crate::wal::heap::Row;

struct OpenTx {
    first_lsn: u64,
    rows: Vec<Row>,
}

#[derive(Default)]
pub struct TxBuffer {
    open: HashMap<u32, OpenTx>,
}

impl TxBuffer {
    pub fn add(&mut self, xid: u32, lsn: u64, row: Row) {
        self.open.entry(xid).or_insert_with(|| OpenTx { first_lsn: lsn, rows: Vec::new() }).rows.push(row);
    }

    pub fn add_many(&mut self, xid: u32, lsn: u64, rows: Vec<Row>) {
        self.open
            .entry(xid)
            .or_insert_with(|| OpenTx { first_lsn: lsn, rows: Vec::new() })
            .rows
            .extend(rows);
    }

    /// Transaction committed: hand back its rows (and those of its committed
    /// subtransactions, which buffer under their own xids) for the sink.
    pub fn commit(&mut self, xid: u32, subxids: &[u32]) -> Vec<Row> {
        let mut rows = self.open.remove(&xid).map(|t| t.rows).unwrap_or_default();
        for sub in subxids {
            if let Some(t) = self.open.remove(sub) {
                rows.extend(t.rows);
            }
        }
        rows
    }

    /// Transaction aborted (or savepoint rolled back — the abort record's
    /// xid is then the subxact's): its rows must never reach the lake.
    pub fn abort(&mut self, xid: u32, subxids: &[u32]) -> usize {
        let mut n = self.open.remove(&xid).map(|t| t.rows.len()).unwrap_or(0);
        for sub in subxids {
            n += self.open.remove(sub).map(|t| t.rows.len()).unwrap_or(0);
        }
        n
    }

    /// First-change LSN of the oldest open transaction — the earliest point
    /// the WAL must still be readable from after a restart.
    pub fn oldest_first_lsn(&self) -> Option<u64> {
        self.open.values().map(|t| t.first_lsn).min()
    }

    pub fn open_count(&self) -> usize {
        self.open.len()
    }
}
