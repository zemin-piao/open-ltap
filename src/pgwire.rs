//! Minimal Postgres frontend-protocol client for *physical* streaming
//! replication. Deliberately tiny: trust or cleartext-password auth, simple
//! queries, CopyBoth streaming. Neon safekeepers speak this same protocol —
//! they differ only in the startup parameters (tenant_id/timeline_id), the
//! START_REPLICATION syntax (no SLOT/TIMELINE clause), and slot commands
//! (none: safekeepers manage their own WAL horizon).
//!
//! Also speaks the Neon pageserver's `pagestream_v3` sub-protocol (GetPage@LSN
//! et al.): the same libpq framing, but a plain (non-replication) startup and
//! request/response messages inside CopyBoth. Message layouts per
//! `libs/pageserver_api/src/pagestream_api.rs` in neondatabase/neon.

use anyhow::{Context, Result, bail};
use bytes::{Buf, BufMut, BytesMut};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Microseconds between the unix epoch and Postgres' 2000-01-01 epoch.
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

pub fn fmt_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

pub fn parse_lsn(s: &str) -> Result<u64> {
    let (hi, lo) = s.split_once('/').context("bad LSN format")?;
    Ok((u64::from_str_radix(hi, 16)? << 32) | u64::from_str_radix(lo, 16)?)
}

fn now_pg_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64 - PG_EPOCH_OFFSET_US)
        .unwrap_or(0)
}

pub struct XLogData {
    pub start_lsn: u64,
    pub wal_end: u64,
    pub data: BytesMut,
}

pub enum ReplMsg {
    XLogData(XLogData),
    Keepalive { wal_end: u64, reply_requested: bool },
}

/// Physical identity of a relation fork, as the pageserver addresses pages.
/// `spcnode` is the tablespace oid (1663 = pg_default), `forknum` 0 = main.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RelTag {
    pub spcnode: u32,
    pub dbnode: u32,
    pub relnode: u32,
    pub forknum: u8,
}

pub struct ReplConn {
    stream: TcpStream,
    buf: BytesMut,
    reqid: u64,
}

impl ReplConn {
    /// `extra_params` are appended to the startup packet (safekeepers take
    /// tenant_id/timeline_id here); `password` answers a cleartext password
    /// request (safekeepers with auth enabled take the JWT there).
    pub async fn connect(
        host: &str,
        port: u16,
        user: &str,
        extra_params: &[(&str, &str)],
        password: Option<&str>,
    ) -> Result<Self> {
        let stream = TcpStream::connect((host, port))
            .await
            .with_context(|| format!("connecting to {host}:{port}"))?;
        let mut conn = ReplConn { stream, buf: BytesMut::with_capacity(64 * 1024), reqid: 0 };
        conn.send_startup(user, true, extra_params).await?;
        conn.await_ready(password).await?;
        Ok(conn)
    }

    /// Connect to a pageserver's page service and enter the `pagestream_v3`
    /// sub-protocol for `tenant`/`timeline`: a plain (non-replication) startup,
    /// then request/response inside CopyBoth. `password` answers a cleartext
    /// password request (JWT when auth is enabled).
    pub async fn connect_pageserver(
        host: &str,
        port: u16,
        user: &str,
        tenant: &str,
        timeline: &str,
        password: Option<&str>,
    ) -> Result<Self> {
        let stream = TcpStream::connect((host, port))
            .await
            .with_context(|| format!("connecting to pageserver {host}:{port}"))?;
        let mut conn = ReplConn { stream, buf: BytesMut::with_capacity(64 * 1024), reqid: 0 };
        conn.send_startup(user, false, &[]).await?;
        conn.await_ready(password).await?;
        conn.send_query(&format!("pagestream_v3 {tenant} {timeline}")).await?;
        conn.await_copy_both().await.context("entering pagestream")?;
        Ok(conn)
    }

    async fn send_startup(
        &mut self,
        user: &str,
        replication: bool,
        extra_params: &[(&str, &str)],
    ) -> Result<()> {
        let mut body = BytesMut::new();
        body.put_u32(196608); // protocol 3.0
        let mut base = vec![("user", user)];
        if replication {
            base.push(("replication", "true"));
        }
        for (k, v) in base.iter().chain(extra_params) {
            body.put_slice(k.as_bytes());
            body.put_u8(0);
            body.put_slice(v.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0);
        let mut msg = BytesMut::new();
        msg.put_u32(body.len() as u32 + 4);
        msg.extend_from_slice(&body);
        self.stream.write_all(&msg).await?;
        Ok(())
    }

    async fn await_ready(&mut self, password: Option<&str>) -> Result<()> {
        loop {
            let (tag, mut payload) = self.read_msg().await?;
            match tag {
                b'R' => {
                    let code = payload.get_u32();
                    match (code, password) {
                        (0, _) => {} // AuthenticationOk
                        (3, Some(pw)) => {
                            // AuthenticationCleartextPassword
                            let mut msg = BytesMut::new();
                            msg.put_u8(b'p');
                            msg.put_u32(4 + pw.len() as u32 + 1);
                            msg.put_slice(pw.as_bytes());
                            msg.put_u8(0);
                            self.stream.write_all(&msg).await?;
                        }
                        (3, None) => bail!("server wants a password and none is configured"),
                        _ => bail!(
                            "server requires auth method {code}; dev setup expects \
                             `host replication ... trust` in pg_hba.conf"
                        ),
                    }
                }
                b'S' | b'K' | b'N' => {} // ParameterStatus / BackendKeyData / Notice
                b'Z' => return Ok(()),
                b'E' => bail!("server error: {}", parse_error(&payload)),
                t => bail!("unexpected message '{}' during startup", t as char),
            }
        }
    }

    /// IDENTIFY_SYSTEM -> (timeline_id, current flush LSN)
    pub async fn identify_system(&mut self) -> Result<(u32, u64)> {
        self.send_query("IDENTIFY_SYSTEM").await?;
        let mut row: Option<Vec<Option<String>>> = None;
        loop {
            let (tag, payload) = self.read_msg().await?;
            match tag {
                b'T' | b'C' => {}
                b'D' => row = Some(parse_data_row(payload)?),
                b'Z' => break,
                b'E' => bail!("IDENTIFY_SYSTEM failed: {}", parse_error(&payload)),
                t => bail!("unexpected message '{}' in IDENTIFY_SYSTEM", t as char),
            }
        }
        let row = row.context("IDENTIFY_SYSTEM returned no row")?;
        let tli: u32 = row
            .get(1)
            .and_then(|f| f.as_deref())
            .context("missing timeline")?
            .parse()?;
        let lsn = parse_lsn(row.get(2).and_then(|f| f.as_deref()).context("missing xlogpos")?)?;
        Ok((tli, lsn))
    }

    /// Create a physical replication slot so the server retains WAL while
    /// we're down. Idempotent: an already-existing slot (42710) is fine.
    pub async fn create_slot(&mut self, name: &str) -> Result<bool> {
        let q = format!("CREATE_REPLICATION_SLOT {name} PHYSICAL RESERVE_WAL");
        self.send_query(&q).await?;
        let mut created = true;
        let mut err: Option<String> = None;
        loop {
            let (tag, payload) = self.read_msg().await?;
            match tag {
                b'T' | b'D' | b'C' | b'N' => {}
                b'E' => {
                    let (code, msg) = parse_error_fields(&payload);
                    if code.as_deref() == Some("42710") {
                        created = false; // duplicate_object: slot already exists
                    } else {
                        err = Some(msg);
                    }
                }
                b'Z' => break,
                t => bail!("unexpected message '{}' in CREATE_REPLICATION_SLOT", t as char),
            }
        }
        if let Some(e) = err {
            bail!("CREATE_REPLICATION_SLOT failed: {e}");
        }
        Ok(created)
    }

    /// Where an existing slot retains WAL from (READ_REPLICATION_SLOT, PG15+).
    pub async fn slot_restart_lsn(&mut self, name: &str) -> Result<Option<u64>> {
        self.send_query(&format!("READ_REPLICATION_SLOT {name}")).await?;
        let mut row: Option<Vec<Option<String>>> = None;
        loop {
            let (tag, payload) = self.read_msg().await?;
            match tag {
                b'T' | b'C' | b'N' => {}
                b'D' => row = Some(parse_data_row(payload)?),
                b'Z' => break,
                b'E' => bail!("READ_REPLICATION_SLOT failed: {}", parse_error(&payload)),
                t => bail!("unexpected message '{}' in READ_REPLICATION_SLOT", t as char),
            }
        }
        // Columns: slot_type, restart_lsn, restart_tli (all NULL if slot is unused).
        Ok(match row.and_then(|r| r.into_iter().nth(1).flatten()) {
            Some(lsn) => Some(parse_lsn(&lsn)?),
            None => None,
        })
    }

    /// Enter CopyBoth mode streaming physical WAL from `start_lsn`
    /// (must be a WAL page boundary for the reader to sync).
    pub async fn start_replication(&mut self, slot: &str, start_lsn: u64, timeline: u32) -> Result<()> {
        let q = format!(
            "START_REPLICATION SLOT {} PHYSICAL {} TIMELINE {}",
            slot,
            fmt_lsn(start_lsn),
            timeline
        );
        self.send_query(&q).await?;
        self.await_copy_both().await
    }

    /// Safekeeper variant: no slot, no TIMELINE clause (the timeline was
    /// already fixed by the startup parameters).
    pub async fn start_replication_safekeeper(&mut self, start_lsn: u64) -> Result<()> {
        self.send_query(&format!("START_REPLICATION PHYSICAL {}", fmt_lsn(start_lsn))).await?;
        self.await_copy_both().await
    }

    async fn await_copy_both(&mut self) -> Result<()> {
        loop {
            let (tag, payload) = self.read_msg().await?;
            match tag {
                b'W' => return Ok(()), // CopyBothResponse
                b'N' => {}
                b'E' => bail!("START_REPLICATION failed: {}", parse_error(&payload)),
                t => bail!("unexpected message '{}' starting replication", t as char),
            }
        }
    }

    pub async fn next_msg(&mut self) -> Result<ReplMsg> {
        loop {
            let (tag, mut payload) = self.read_msg().await?;
            match tag {
                b'd' => {
                    let kind = payload.get_u8();
                    match kind {
                        b'w' => {
                            let start_lsn = payload.get_u64();
                            let wal_end = payload.get_u64();
                            let _send_time = payload.get_i64();
                            return Ok(ReplMsg::XLogData(XLogData { start_lsn, wal_end, data: payload }));
                        }
                        b'k' => {
                            let wal_end = payload.get_u64();
                            let _send_time = payload.get_i64();
                            let reply_requested = payload.get_u8() != 0;
                            return Ok(ReplMsg::Keepalive { wal_end, reply_requested });
                        }
                        k => bail!("unexpected CopyData kind '{}'", k as char),
                    }
                }
                b'N' => {}
                b'E' => bail!("stream error: {}", parse_error(&payload)),
                b'c' | b'Z' => bail!("server ended the replication stream"),
                t => bail!("unexpected message '{}' in stream", t as char),
            }
        }
    }

    /// Standby status update. `flushed` is what the slot's restart_lsn tracks:
    /// report only what is durably in Delta, so the server retains everything
    /// we might still need to replay after a crash.
    pub async fn send_status(&mut self, received: u64, flushed: u64) -> Result<()> {
        let mut msg = BytesMut::new();
        msg.put_u8(b'd');
        msg.put_u32(4 + 1 + 8 * 3 + 8 + 1);
        msg.put_u8(b'r');
        msg.put_u64(received);
        msg.put_u64(flushed);
        msg.put_u64(flushed); // applied
        msg.put_i64(now_pg_micros());
        msg.put_u8(0); // no reply requested
        self.stream.write_all(&msg).await?;
        Ok(())
    }

    // ---- pagestream_v3 requests (connection must be from connect_pageserver) ----
    //
    // Every request is one CopyData frame: tag u8, then reqid/request_lsn/
    // not_modified_since (u64 BE each), then per-type fields. The response
    // echoes the whole request header + fields before its payload. We always
    // send not_modified_since = request_lsn (documented as always correct);
    // the pageserver waits for WAL up to that LSN to arrive, so callers must
    // pass an LSN the safekeepers have actually committed.

    /// Fetch the 8KB page of `rel` block `blkno` materialized at `lsn`.
    pub async fn get_page(&mut self, rel: RelTag, blkno: u32, lsn: u64) -> Result<Vec<u8>> {
        let mut req = self.pagestream_hdr(2, lsn);
        put_reltag(&mut req, rel);
        req.put_u32(blkno);
        let mut resp = self.pagestream_roundtrip(req, 102, "GetPage").await?;
        resp.advance(33); // echoed request_lsn + not_modified_since + reltag + blkno
        if resp.len() != 8192 {
            bail!("GetPage returned {} bytes, want 8192", resp.len());
        }
        Ok(resp.to_vec())
    }

    /// Size of `rel` in blocks at `lsn`.
    pub async fn rel_nblocks(&mut self, rel: RelTag, lsn: u64) -> Result<u32> {
        let mut req = self.pagestream_hdr(1, lsn);
        put_reltag(&mut req, rel);
        let mut resp = self.pagestream_roundtrip(req, 101, "Nblocks").await?;
        resp.advance(29); // echoed lsns + reltag
        Ok(resp.get_u32())
    }

    /// Whether `rel` exists at `lsn`.
    pub async fn rel_exists(&mut self, rel: RelTag, lsn: u64) -> Result<bool> {
        let mut req = self.pagestream_hdr(0, lsn);
        put_reltag(&mut req, rel);
        let mut resp = self.pagestream_roundtrip(req, 100, "Exists").await?;
        resp.advance(29); // echoed lsns + reltag
        Ok(resp.get_u8() != 0)
    }

    fn pagestream_hdr(&mut self, tag: u8, lsn: u64) -> BytesMut {
        self.reqid += 1;
        let mut req = BytesMut::with_capacity(64);
        req.put_u8(tag);
        req.put_u64(self.reqid);
        req.put_u64(lsn); // request_lsn
        req.put_u64(lsn); // not_modified_since
        req
    }

    /// Send one request frame, read one response frame, check the tag and the
    /// echoed reqid, and return the payload positioned after the reqid.
    async fn pagestream_roundtrip(
        &mut self,
        req: BytesMut,
        want_tag: u8,
        what: &str,
    ) -> Result<BytesMut> {
        let mut msg = BytesMut::with_capacity(req.len() + 5);
        msg.put_u8(b'd');
        msg.put_u32(4 + req.len() as u32);
        msg.extend_from_slice(&req);
        self.stream.write_all(&msg).await?;

        let mut resp = loop {
            let (tag, payload) = self.read_msg().await?;
            match tag {
                b'd' => break payload,
                b'N' => {}
                b'E' => bail!("{what} failed: {}", parse_error(&payload)),
                b'c' | b'Z' => bail!("server ended the pagestream"),
                t => bail!("unexpected message '{}' in pagestream", t as char),
            }
        };
        if resp.len() < 25 {
            bail!("{what}: truncated pagestream response ({} bytes)", resp.len());
        }
        let tag = resp.get_u8();
        let got_reqid = resp.get_u64();
        if tag == 103 {
            // Error: echoed lsns, then a NUL-terminated message.
            resp.advance(16);
            let end = resp.iter().position(|&b| b == 0).unwrap_or(resp.len());
            bail!("{what}: pageserver error: {}", String::from_utf8_lossy(&resp[..end]));
        }
        if tag != want_tag {
            bail!("{what}: unexpected pagestream response tag {tag}");
        }
        if got_reqid != self.reqid {
            bail!("{what}: response for reqid {got_reqid}, want {}", self.reqid);
        }
        Ok(resp)
    }

    async fn send_query(&mut self, q: &str) -> Result<()> {
        let mut msg = BytesMut::new();
        msg.put_u8(b'Q');
        msg.put_u32(4 + q.len() as u32 + 1);
        msg.put_slice(q.as_bytes());
        msg.put_u8(0);
        self.stream.write_all(&msg).await?;
        Ok(())
    }

    async fn read_msg(&mut self) -> Result<(u8, BytesMut)> {
        loop {
            if self.buf.len() >= 5 {
                let tag = self.buf[0];
                let len =
                    u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
                if len < 4 {
                    bail!("corrupt message length");
                }
                if self.buf.len() >= 1 + len {
                    self.buf.advance(5);
                    let payload = self.buf.split_to(len - 4);
                    return Ok((tag, payload));
                }
            }
            let n = self.stream.read_buf(&mut self.buf).await?;
            if n == 0 {
                bail!("connection closed by server");
            }
        }
    }
}

fn put_reltag(buf: &mut BytesMut, rel: RelTag) {
    buf.put_u32(rel.spcnode);
    buf.put_u32(rel.dbnode);
    buf.put_u32(rel.relnode);
    buf.put_u8(rel.forknum);
}

fn parse_data_row(mut payload: BytesMut) -> Result<Vec<Option<String>>> {
    let n = payload.get_u16();
    let mut fields = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let len = payload.get_i32();
        if len < 0 {
            fields.push(None);
        } else {
            let bytes = payload.split_to(len as usize);
            fields.push(Some(String::from_utf8_lossy(&bytes).into_owned()));
        }
    }
    Ok(fields)
}

fn parse_error(payload: &[u8]) -> String {
    parse_error_fields(payload).1
}

/// ErrorResponse: sequence of (field_type: u8, cstring) pairs, 0-terminated.
/// Returns (sqlstate code, human-readable message).
fn parse_error_fields(payload: &[u8]) -> (Option<String>, String) {
    let mut msg = String::new();
    let mut code = None;
    let mut i = 0;
    while i < payload.len() && payload[i] != 0 {
        let field = payload[i];
        i += 1;
        let start = i;
        while i < payload.len() && payload[i] != 0 {
            i += 1;
        }
        let val = String::from_utf8_lossy(&payload[start..i]);
        match field {
            b'M' | b'S' => {
                if !msg.is_empty() {
                    msg.push_str(": ");
                }
                msg.push_str(&val);
            }
            b'C' => code = Some(val.into_owned()),
            _ => {}
        }
        i += 1;
    }
    (code, if msg.is_empty() { "unknown error".into() } else { msg })
}
