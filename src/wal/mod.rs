//! Physical WAL stream parsing: reassembles XLogRecords from the raw byte
//! stream (stripping 8KB page headers, handling records that span pages),
//! then parses the record header + block references.
//!
//! References: postgres src/include/access/xlogrecord.h and
//! src/backend/access/transam/xlogreader.c. Little-endian only (WAL is
//! native-endian of the server; both dev containers and Apple/x86 hosts are LE).

pub mod heap;

use anyhow::{Result, bail};
use bytes::{Buf, BytesMut};

pub const XLOG_PAGE_SIZE: u64 = 8192;
/// Heap page size (BLCKSZ); same as the WAL page size on stock builds.
pub const XLOG_BLCKSZ: usize = 8192;
pub const WAL_SEG_SIZE: u64 = 16 * 1024 * 1024;
/// SizeOfXLogRecord: the fixed record header. May be split across pages;
/// only its first field (xl_tot_len) is guaranteed on-page.
const REC_HDR_LEN: usize = 24;
const SHORT_PAGE_HDR: usize = 24; // SizeOfXLogShortPHD (maxaligned)
const LONG_PAGE_HDR: usize = 40; // SizeOfXLogLongPHD (segment start)
/// xlp_magic values we have verified the decoder against (both versions
/// share the record, heap, and xact layouts we parse).
const XLOG_PAGE_MAGICS: &[(u16, u32)] = &[(0xD116, 17), (0xD118, 18)];

fn maxalign(v: u64) -> u64 {
    (v + 7) & !7
}

/// Streaming record reassembler. Feed it XLogData payloads (which are raw
/// WAL bytes at an absolute LSN); it yields complete records tagged with
/// their start LSN.
pub struct WalReader {
    /// LSN of the next unconsumed byte in `pending`.
    pos: u64,
    pending: BytesMut,
    /// Bytes to discard (continuation of a record from before our start
    /// point, or inter-record alignment padding).
    skip_left: u64,
    started: bool,
    rec: Vec<u8>,
    rec_lsn: u64,
    need: usize,
}

impl WalReader {
    /// `start_lsn` must be page-aligned (round your start position down).
    pub fn new(start_lsn: u64) -> Self {
        assert_eq!(start_lsn % XLOG_PAGE_SIZE, 0, "start LSN must be page-aligned");
        WalReader {
            pos: start_lsn,
            pending: BytesMut::new(),
            skip_left: 0,
            started: false,
            rec: Vec::new(),
            rec_lsn: 0,
            need: 0,
        }
    }

    pub fn feed(&mut self, start_lsn: u64, data: &[u8]) -> Result<Vec<(u64, Vec<u8>)>> {
        let expected = self.pos + self.pending.len() as u64;
        if start_lsn != expected {
            bail!(
                "WAL stream gap: expected {}, got {}",
                crate::pgwire::fmt_lsn(expected),
                crate::pgwire::fmt_lsn(start_lsn)
            );
        }
        self.pending.extend_from_slice(data);
        let mut out = Vec::new();

        loop {
            let avail = self.pending.len();
            if avail == 0 {
                break;
            }
            let page_off = (self.pos % XLOG_PAGE_SIZE) as usize;

            // Page boundary: strip the page header.
            if page_off == 0 {
                let hdr_len =
                    if self.pos % WAL_SEG_SIZE == 0 { LONG_PAGE_HDR } else { SHORT_PAGE_HDR };
                if avail < hdr_len {
                    break;
                }
                // xlp_magic: bumped every major release. Checking every page
                // both rejects unsupported server versions up front and acts
                // as a desync guard for the reader's position tracking.
                let magic = u16::from_le_bytes(self.pending[0..2].try_into().unwrap());
                if !XLOG_PAGE_MAGICS.iter().any(|(m, _)| *m == magic) {
                    bail!(
                        "unsupported WAL page magic {magic:#06x} at {} (supported: {})",
                        crate::pgwire::fmt_lsn(self.pos),
                        XLOG_PAGE_MAGICS
                            .iter()
                            .map(|(m, v)| format!("{m:#06x}=PG{v}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
                let rem_len =
                    u32::from_le_bytes(self.pending[16..20].try_into().unwrap()) as u64;
                if !self.started {
                    // We joined mid-stream: skip the tail of whatever record
                    // was in flight at our (page-aligned) start position.
                    self.started = true;
                    self.skip_left = rem_len;
                }
                self.pending.advance(hdr_len);
                self.pos += hdr_len as u64;
                if self.skip_left == 0 && self.need == 0 {
                    // Records begin maxaligned after the header (both header
                    // sizes are already 8-aligned, so this is a no-op; kept
                    // for clarity).
                    let pad = maxalign(self.pos) - self.pos;
                    self.skip_left = pad;
                }
                continue;
            }

            let page_rem = XLOG_PAGE_SIZE as usize - page_off;

            // Discard continuation/padding bytes.
            if self.skip_left > 0 {
                let take = (self.skip_left as usize).min(page_rem).min(avail);
                self.pending.advance(take);
                self.pos += take as u64;
                self.skip_left -= take as u64;
                if self.skip_left == 0 {
                    self.skip_left = maxalign(self.pos) - self.pos;
                }
                continue;
            }

            // Mid-record: keep accumulating the body.
            if self.need > 0 {
                let take = (self.need - self.rec.len()).min(page_rem).min(avail);
                self.rec.extend_from_slice(&self.pending[..take]);
                self.pending.advance(take);
                self.pos += take as u64;
                if self.rec.len() == self.need {
                    out.push((self.rec_lsn, std::mem::take(&mut self.rec)));
                    self.need = 0;
                    self.skip_left = maxalign(self.pos) - self.pos;
                }
                continue;
            }

            // New record. Records start maxaligned, so the remaining space on
            // the page is a multiple of 8 and xl_tot_len (the first 4 bytes)
            // is always on this page — but the REST of the 24-byte header may
            // continue on the next page (xlogreader.c). The `need` path below
            // reassembles header and body alike across pages.
            if avail < 4 {
                break;
            }
            let tot_len = u32::from_le_bytes(self.pending[0..4].try_into().unwrap()) as usize;
            if tot_len == 0 {
                // Zero padding up to the next page.
                let take = page_rem.min(avail);
                self.pending.advance(take);
                self.pos += take as u64;
                continue;
            }
            if tot_len < REC_HDR_LEN {
                bail!("corrupt WAL: xl_tot_len={} at {}", tot_len, crate::pgwire::fmt_lsn(self.pos));
            }
            self.rec_lsn = self.pos;
            self.need = tot_len;
            self.rec.clear();
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Record parsing (header + block references + data payloads)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelTag {
    pub spc: u32,
    pub db: u32,
    pub rel: u32,
}

/// A full-page image as carried in the record: possibly compressed, with the
/// pd_lower..pd_upper "hole" elided. `restore()` yields the 8192-byte page.
#[derive(Debug)]
pub struct PageImage {
    pub data: Vec<u8>,
    pub hole_offset: u16,
    pub hole_len: u16,
    pub bimg_info: u8,
}

impl PageImage {
    pub fn restore(&self) -> Result<Vec<u8>> {
        let body: std::borrow::Cow<[u8]> = match self.bimg_info & BKPIMAGE_COMPRESS_MASK {
            0 => self.data.as_slice().into(),
            BKPIMAGE_COMPRESS_PGLZ => {
                let raw = XLOG_BLCKSZ - self.hole_len as usize;
                heap::pglz_decompress(&self.data, raw)?.into()
            }
            m => bail!("unsupported page-image compression {m:#x} (set wal_compression=off or pglz)"),
        };
        let hole_off = self.hole_offset as usize;
        let hole_len = self.hole_len as usize;
        if body.len() + hole_len != XLOG_BLCKSZ || hole_off + hole_len > XLOG_BLCKSZ {
            bail!("bad page image geometry: {} bytes + {hole_len} hole", body.len());
        }
        let mut page = Vec::with_capacity(XLOG_BLCKSZ);
        page.extend_from_slice(&body[..hole_off]);
        page.resize(hole_off + hole_len, 0);
        page.extend_from_slice(&body[hole_off..]);
        Ok(page)
    }
}

#[derive(Debug)]
pub struct BlockRef {
    pub id: u8,
    pub rel: RelTag,
    pub blkno: u32,
    pub image: Option<PageImage>,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct Record {
    pub xid: u32,
    pub rmid: u8,
    pub info: u8,
    pub blocks: Vec<BlockRef>,
    pub main_data: Vec<u8>,
}

pub mod rmgr {
    pub const XACT: u8 = 1;
    pub const SMGR: u8 = 2;
    pub const HEAP2: u8 = 9;
    pub const HEAP: u8 = 10;
}

// fork_flags bits (xlogrecord.h)
const BKPBLOCK_HAS_IMAGE: u8 = 0x10;
const BKPBLOCK_HAS_DATA: u8 = 0x20;
const BKPBLOCK_SAME_REL: u8 = 0x80;
// bimg_info bits
const BKPIMAGE_HAS_HOLE: u8 = 0x01;
const BKPIMAGE_COMPRESS_PGLZ: u8 = 0x04;
const BKPIMAGE_COMPRESS_MASK: u8 = 0x04 | 0x08 | 0x10; // pglz | lz4 | zstd

const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
const XLR_BLOCK_ID_ORIGIN: u8 = 253;
const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;
const XLR_MAX_BLOCK_ID: u8 = 32;

struct Cursor<'a> {
    buf: &'a [u8],
    off: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.off + n > self.buf.len() {
            bail!("record truncated: need {} bytes at offset {}", n, self.off);
        }
        let s = &self.buf[self.off..self.off + n];
        self.off += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn remaining(&self) -> usize {
        self.buf.len() - self.off
    }
}

pub fn parse_record(rec: &[u8]) -> Result<Record> {
    if rec.len() < REC_HDR_LEN {
        bail!("record shorter than header");
    }
    let xid = u32::from_le_bytes(rec[4..8].try_into().unwrap());
    let info = rec[16];
    let rmid = rec[17];

    // xl_crc covers the record body, then the header up to (not including)
    // the crc field itself (xloginsert.c).
    let stored_crc = u32::from_le_bytes(rec[20..24].try_into().unwrap());
    let crc = crc32c::crc32c_append(crc32c::crc32c(&rec[REC_HDR_LEN..]), &rec[0..20]);
    if crc != stored_crc {
        bail!("CRC mismatch: computed {crc:08x}, stored {stored_crc:08x}");
    }

    let mut c = Cursor { buf: rec, off: REC_HDR_LEN };

    struct HdrBlock {
        id: u8,
        rel: RelTag,
        blkno: u32,
        has_image: bool,
        image_len: usize,
        hole_offset: u16,
        hole_len: u16,
        bimg_info: u8,
        has_data: bool,
        data_len: usize,
    }
    let mut hdr_blocks: Vec<HdrBlock> = Vec::new();
    let mut main_len = 0usize;
    let mut datatotal = 0usize;
    let mut prev_rel: Option<RelTag> = None;

    while c.remaining() > datatotal {
        let block_id = c.u8()?;
        match block_id {
            XLR_BLOCK_ID_DATA_SHORT => {
                main_len = c.u8()? as usize;
                datatotal += main_len;
            }
            XLR_BLOCK_ID_DATA_LONG => {
                main_len = c.u32()? as usize;
                datatotal += main_len;
            }
            XLR_BLOCK_ID_ORIGIN => {
                c.take(2)?;
            }
            XLR_BLOCK_ID_TOPLEVEL_XID => {
                c.take(4)?;
            }
            id if id <= XLR_MAX_BLOCK_ID => {
                let fork_flags = c.u8()?;
                let data_len = c.u16()? as usize;
                let has_image = fork_flags & BKPBLOCK_HAS_IMAGE != 0;
                let has_data = fork_flags & BKPBLOCK_HAS_DATA != 0;
                let mut image_len = 0usize;
                let mut hole_offset = 0u16;
                let mut hole_len = 0u16;
                let mut bimg_info = 0u8;
                if has_image {
                    image_len = c.u16()? as usize;
                    hole_offset = c.u16()?;
                    bimg_info = c.u8()?;
                    if bimg_info & BKPIMAGE_HAS_HOLE != 0 {
                        // Compressed images carry the hole length explicitly;
                        // raw images imply it from the elided bytes.
                        hole_len = if bimg_info & BKPIMAGE_COMPRESS_MASK != 0 {
                            c.u16()?
                        } else {
                            (XLOG_BLCKSZ - image_len) as u16
                        };
                    }
                    datatotal += image_len;
                }
                let rel = if fork_flags & BKPBLOCK_SAME_REL != 0 {
                    prev_rel.ok_or_else(|| anyhow::anyhow!("SAME_REL with no previous block"))?
                } else {
                    RelTag { spc: c.u32()?, db: c.u32()?, rel: c.u32()? }
                };
                prev_rel = Some(rel);
                let blkno = c.u32()?;
                if has_data {
                    datatotal += data_len;
                }
                hdr_blocks.push(HdrBlock {
                    id, rel, blkno, has_image, image_len, hole_offset, hole_len, bimg_info,
                    has_data, data_len,
                });
            }
            id => bail!("invalid block_id {id} in record"),
        }
    }

    // Data section: per block (image then data), then main data.
    let mut blocks = Vec::with_capacity(hdr_blocks.len());
    for hb in hdr_blocks {
        let image = if hb.has_image {
            Some(PageImage {
                data: c.take(hb.image_len)?.to_vec(),
                hole_offset: hb.hole_offset,
                hole_len: hb.hole_len,
                bimg_info: hb.bimg_info,
            })
        } else {
            None
        };
        let data = if hb.has_data { c.take(hb.data_len)?.to_vec() } else { Vec::new() };
        blocks.push(BlockRef { id: hb.id, rel: hb.rel, blkno: hb.blkno, image, data });
    }
    let main_data = c.take(main_len)?.to_vec();

    Ok(Record { xid, rmid, info, blocks, main_data })
}
