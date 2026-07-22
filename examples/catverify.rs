//! Live verification of catalog-from-pages + CLOG visibility against a real
//! on-disk PostgreSQL data directory (no server needed — reads the files).
//!
//!   catverify clog    <datadir> <xid>...           resolve xids via pg_xact
//!   catverify catalog <datadir> <dboid> <table>    derive a TableDesc from pages

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use anyhow::{Result, bail};
use open_ltap::catalog::{Catalog, MappedRels, PageSource, parse_relmap};
use open_ltap::clog::{FileClogSource, resolve};

/// PageSource over a database's on-disk relation files (base/<dboid>/<filenode>).
struct FilePageSource {
    db: u32,
    dir: PathBuf,
}

impl PageSource for FilePageSource {
    fn db(&self) -> u32 {
        self.db
    }
    async fn rel_nblocks(&mut self, filenode: u32) -> Result<u32> {
        let p = self.dir.join(filenode.to_string());
        Ok((std::fs::metadata(&p)?.len() / 8192) as u32)
    }
    async fn get_page(&mut self, filenode: u32, blk: u32) -> Result<Vec<u8>> {
        let mut f = std::fs::File::open(self.dir.join(filenode.to_string()))?;
        f.seek(SeekFrom::Start(blk as u64 * 8192))?;
        let mut buf = vec![0u8; 8192];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("clog") => {
            let datadir = PathBuf::from(&args[1]);
            let mut clog = FileClogSource::new(datadir.join("pg_xact"));
            for xid in &args[2..] {
                let xid: u32 = xid.parse()?;
                println!("xid {xid} -> {:?}", resolve(&mut clog, xid).await?);
            }
        }
        Some("catalog") => {
            let datadir = PathBuf::from(&args[1]);
            let dboid: u32 = args[2].parse()?;
            let table = &args[3];
            let base = datadir.join("base").join(dboid.to_string());

            let relmap = parse_relmap(&std::fs::read(base.join("pg_filenode.map"))?)?;
            let mapped = MappedRels::from_relmap(&relmap)?;
            println!(
                "relmapper: pg_class={} pg_attribute={} pg_index={:?}",
                mapped.pg_class, mapped.pg_attribute, mapped.pg_index
            );

            let mut src = FilePageSource { db: dboid, dir: base };
            let mut clog = FileClogSource::new(datadir.join("pg_xact"));
            let cat = Catalog::load(&mut src, &mut clog, &mapped).await?;

            println!("public tables (visibility-filtered): {:?}", cat.table_names());
            let d = cat.desc(table)?;
            println!("desc({table}): oid={} filenode={} pk={:?}", d.oid, d.rel_node, d.pk);
            for c in &d.cols {
                println!("  {} : {:?}", c.name, c.ty);
            }
        }
        _ => bail!("usage: catverify (clog <datadir> <xid>... | catalog <datadir> <dboid> <table>)"),
    }
    Ok(())
}
