//! Live harness for `crate::multixact::resolve_updater` — reads a real
//! `pg_multixact/` directory and reports the updater member of a given
//! MultiXactId, if any. Companion to `clogvis.rs`; exercises
//! `FileMultiXactSource`'s real segment-file addressing (the unit tests in
//! `src/multixact.rs` pin the packed layout with hand-built pages, not real
//! files on disk).
//!
//!   cargo run --example mxcheck -- <pg_multixact dir> <multixact id>

use anyhow::{Context, Result};
use open_ltap::multixact::{FileMultiXactSource, resolve_updater};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let dir = args.next().context("usage: mxcheck <pg_multixact dir> <multixact id>")?;
    let multi: u32 = args.next().context("usage: mxcheck <pg_multixact dir> <multixact id>")?.parse()?;

    let mut src = FileMultiXactSource::new(&dir);
    match resolve_updater(&mut src, multi).await? {
        Some(xid) => println!("multixact {multi}: updater xid = {xid}"),
        None => println!("multixact {multi}: no updater (every member is a locker)"),
    }
    Ok(())
}
