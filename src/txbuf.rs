//! Per-transaction change buffering.
//!
//! WAL carries changes from transactions that may later abort; only ops
//! whose transaction reaches a COMMIT record may be applied to the mirror
//! and emitted to the lake. Each open transaction also keeps an *overlay*
//! of its own uncommitted row versions, keyed by ctid, so a later change in
//! the same transaction can find its pre-image before anything is committed.
//!
//! Looking a ctid up across ALL open transactions' overlays is safe: row
//! locks mean two in-flight transactions can never both have changed the
//! same row, and a ctid slot cannot be reused while the deleting transaction
//! is still open. This also gives subtransactions visibility into their
//! parent's changes without knowing the xid tree up front.

use std::collections::HashMap;

use crate::wal::heap::Row;

/// (block number, line-pointer offset) — a tuple's physical address.
pub type Ctid = (u32, u16);

pub fn pack_ctid((blk, off): Ctid) -> i64 {
    ((blk as i64) << 16) | off as i64
}

pub fn unpack_ctid(packed: i64) -> Ctid {
    ((packed >> 16) as u32, (packed & 0xFFFF) as u16)
}

/// A row version: decoded values + the on-page attribute bytes (pre-image
/// for prefix/suffix-compressed updates; None when not reproducible).
#[derive(Clone)]
pub struct RowVersion {
    pub row: Row,
    pub attrs: Option<Vec<u8>>,
}

pub enum Op {
    Insert { ctid: Ctid, ver: RowVersion },
    Update { old_ctid: Ctid, ctid: Ctid, ver: RowVersion },
    Delete { ctid: Ctid, old_row: Option<Row> },
}

pub struct PendingOp {
    pub lsn: u64,
    /// Index into the tracked-table list (multi-table: one transaction's
    /// ops may span several tables).
    pub table: usize,
    pub op: Op,
}

#[derive(Default)]
struct OpenTx {
    first_lsn: u64,
    ops: Vec<PendingOp>,
    overlay: HashMap<(usize, Ctid), RowVersion>,
}

#[derive(Default)]
pub struct TxBuffer {
    open: HashMap<u32, OpenTx>,
}

impl TxBuffer {
    pub fn add_op(&mut self, xid: u32, lsn: u64, table: usize, op: Op) {
        let tx = self.open.entry(xid).or_insert_with(|| OpenTx { first_lsn: lsn, ..Default::default() });
        match &op {
            Op::Insert { ctid, ver } => {
                tx.overlay.insert((table, *ctid), ver.clone());
            }
            Op::Update { old_ctid, ctid, ver } => {
                tx.overlay.remove(&(table, *old_ctid));
                tx.overlay.insert((table, *ctid), ver.clone());
            }
            Op::Delete { ctid, .. } => {
                tx.overlay.remove(&(table, *ctid));
            }
        }
        tx.ops.push(PendingOp { lsn, table, op });
    }

    /// Find an uncommitted version of a row across all open transactions.
    pub fn lookup(&self, table: usize, ctid: Ctid) -> Option<&RowVersion> {
        self.open.values().find_map(|tx| tx.overlay.get(&(table, ctid)))
    }

    /// Transaction committed: hand back its ops (and its committed
    /// subtransactions', which buffer under their own xids) in WAL order.
    pub fn commit(&mut self, xid: u32, subxids: &[u32]) -> Vec<PendingOp> {
        let mut ops = self.open.remove(&xid).map(|t| t.ops).unwrap_or_default();
        for sub in subxids {
            if let Some(t) = self.open.remove(sub) {
                ops.extend(t.ops);
            }
        }
        // Main-xid and subxid records interleave in the WAL; restore that order.
        ops.sort_by_key(|o| o.lsn);
        ops
    }

    /// Transaction aborted (or savepoint rolled back — the abort record's
    /// xid is then the subxact's): its ops must never reach the lake.
    pub fn abort(&mut self, xid: u32, subxids: &[u32]) -> usize {
        let mut n = self.open.remove(&xid).map(|t| t.ops.len()).unwrap_or(0);
        for sub in subxids {
            n += self.open.remove(sub).map(|t| t.ops.len()).unwrap_or(0);
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
