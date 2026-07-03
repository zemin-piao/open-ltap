//! Offline WAL reader check: feed a raw segment file through WalReader +
//! parse_record and print one line per record, comparable to pg_waldump.
//!
//!   cargo run --example walscan -- <segment-file> <start-lsn-hex>  [end-lsn-hex]

use open_ltap::wal::{self, WalReader};

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("segment file");
    let seg_start = u64::from_str_radix(&args.next().expect("segment start lsn (hex)"), 16)?;
    let bytes = std::fs::read(&path)?;

    // Optional third arg: feed in chunks of this size to simulate streaming.
    let chunk: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(bytes.len());

    let mut reader = WalReader::new(seg_start);
    let mut recs = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let end = (pos + chunk).min(bytes.len());
        recs.extend(reader.feed(seg_start + pos as u64, &bytes[pos..end])?);
        pos = end;
    }
    for (lsn, rec) in recs {
        match wal::parse_record(&rec) {
            Ok(r) => println!(
                "lsn {:X}/{:X} rmid {} info {:#04x} xid {} len {} blocks {}",
                lsn >> 32,
                lsn & 0xFFFF_FFFF,
                r.rmid,
                r.info,
                r.xid,
                rec.len(),
                r.blocks.len()
            ),
            Err(e) => println!("lsn {:X}/{:X} len {} PARSE-ERROR {e}", lsn >> 32, lsn & 0xFFFF_FFFF, rec.len()),
        }
    }
    Ok(())
}
