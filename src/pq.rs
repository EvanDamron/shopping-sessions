//! A minimal Parquet writer for flat tables: one row group, one PLAIN data page per
//! column, no compression, no statistics. Thrift compact protocol written by hand.
//! `write_raw` takes columns that are already PLAIN-encoded, so the hot path encodes
//! rows once, as it produces them, and the file is written without another copy.

use std::io::Write;

pub enum Col<'a> {
    /// Required UTF-8 strings.
    Str(Vec<&'a [u8]>),
    /// Optional UTF-8 strings (None = null).
    OptStr(Vec<Option<&'a [u8]>>),
    /// Required INT64.
    I64(Vec<i64>),
    /// Required DATE (INT32 days since the epoch).
    Date(Vec<i32>),
}

struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl Thrift {
    fn new() -> Self {
        Thrift { buf: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let delta = id - last;
        if delta > 0 && delta <= 15 {
            self.buf.push(((delta as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            self.zigzag(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zigzag(v);
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(v.len() as u64);
        self.buf.extend_from_slice(v);
    }
    fn begin_struct(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }
    fn begin_list_elem_struct(&mut self) {
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
}

const TYPE_INT32: i32 = 1;
const TYPE_INT64: i32 = 2;
const TYPE_BYTE_ARRAY: i32 = 6;


/// A column whose values are already encoded.
pub enum Raw<'a> {
    /// Required strings: PLAIN BYTE_ARRAY values (4-byte length, bytes).
    Str(&'a [u8]),
    /// Optional strings: one bit per row (least significant first, 1 = present) and
    /// the PLAIN values of the rows that are present.
    OptStr(&'a [u8], &'a [u8]),
    /// Required INT32, little-endian.
    I32(&'a [u8]),
    /// Required INT64, little-endian.
    I64(&'a [u8]),
    /// Required DATE (INT32 days since the epoch), little-endian.
    Date(&'a [u8]),
}

/// Writes `cols` (each `nrows` long) with the given names to `path`.
pub fn write_raw(path: &std::path::Path, names: &[&str], cols: &[Raw], nrows: usize) -> std::io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    let mut offset = 4usize;
    let mut chunks = Vec::new(); // (type, data_page_offset, total size)
    let mut heads: Vec<Vec<u8>> = Vec::new(); // per column: page header, then any level bytes
    for col in cols.iter().filter(|_| nrows > 0) {
        let mut pre = Vec::new(); // bytes of the page that precede the values
        let (ty, values): (i32, &[u8]) = match col {
            Raw::Str(v) => (TYPE_BYTE_ARRAY, v),
            Raw::OptStr(bits, v) => {
                // Definition levels: RLE/bit-packed hybrid, one bit-packed run, 4-byte length prefix.
                let groups = nrows.div_ceil(8);
                let mut h = Thrift::new();
                h.varint(((groups as u64) << 1) | 1);
                pre.extend_from_slice(&((h.buf.len() + groups) as u32).to_le_bytes());
                pre.extend_from_slice(&h.buf);
                pre.extend_from_slice(&bits[..groups]);
                (TYPE_BYTE_ARRAY, v)
            }
            Raw::I32(v) | Raw::Date(v) => (TYPE_INT32, v),
            Raw::I64(v) => (TYPE_INT64, v),
        };
        let page_len = pre.len() + values.len();
        let mut h = Thrift::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, page_len as i32);
        h.i32(3, page_len as i32);
        h.begin_struct(5); // DataPageHeader
        h.i32(1, nrows as i32);
        h.i32(2, 0); // PLAIN
        h.i32(3, 3); // RLE
        h.i32(4, 3); // RLE
        h.end_struct();
        h.buf.push(0);
        let size = h.buf.len() + page_len;
        chunks.push((ty, offset, size));
        offset += size;
        h.buf.extend_from_slice(&pre);
        heads.push(h.buf);
    }
    // FileMetaData
    let mut m = Thrift::new();
    m.i32(1, 1);
    m.list(2, T_STRUCT, cols.len() + 1);
    m.begin_list_elem_struct();
    m.bin(4, b"schema");
    m.i32(5, cols.len() as i32);
    m.end_struct();
    for (col, name) in cols.iter().zip(names) {
        m.begin_list_elem_struct();
        let (ty, rep, conv, logical): (i32, i32, i32, i16) = match col {
            Raw::Str(_) => (TYPE_BYTE_ARRAY, 0, 0, 1),
            Raw::OptStr(..) => (TYPE_BYTE_ARRAY, 1, 0, 1),
            Raw::I32(_) => (TYPE_INT32, 0, -1, 0),
            Raw::I64(_) => (TYPE_INT64, 0, -1, 0),
            Raw::Date(_) => (TYPE_INT32, 0, 6, 6),
        };
        m.i32(1, ty);
        m.i32(3, rep);
        m.bin(4, name.as_bytes());
        if conv >= 0 {
            m.i32(6, conv); // UTF8 = 0, DATE = 6
        }
        if logical > 0 {
            m.begin_struct(10); // LogicalType union
            m.begin_struct(logical); // STRING = 1, DATE = 6
            m.end_struct();
            m.end_struct();
        }
        m.end_struct();
    }
    m.i64(3, nrows as i64);
    if nrows == 0 {
        m.list(4, T_STRUCT, 0);
    } else {
        m.list(4, T_STRUCT, 1);
        m.begin_list_elem_struct(); // RowGroup
        m.list(1, T_STRUCT, cols.len());
        let total: usize = chunks.iter().map(|c| c.2).sum();
        for ((ty, offset, size), name) in chunks.iter().zip(names) {
            m.begin_list_elem_struct(); // ColumnChunk
            m.i64(2, *offset as i64);
            m.begin_struct(3); // ColumnMetaData
            m.i32(1, *ty);
            m.list(2, T_I32, 2);
            m.zigzag(0); // PLAIN
            m.zigzag(3); // RLE
            m.list(3, T_BINARY, 1);
            m.varint(name.len() as u64);
            m.buf.extend_from_slice(name.as_bytes());
            m.i32(4, 0); // UNCOMPRESSED
            m.i64(5, nrows as i64);
            m.i64(6, *size as i64);
            m.i64(7, *size as i64);
            m.i64(9, *offset as i64);
            m.end_struct();
            m.end_struct();
        }
        m.i64(2, total as i64);
        m.i64(3, nrows as i64);
        m.end_struct();
    }
    m.bin(6, b"sessions");
    m.buf.push(0);
    let mlen = m.buf.len() as u32;
    m.buf.extend_from_slice(&mlen.to_le_bytes());
    m.buf.extend_from_slice(b"PAR1");

    file.write_all(b"PAR1")?;
    if nrows > 0 {
        for (col, head) in cols.iter().zip(&heads) {
            file.write_all(head)?;
            file.write_all(match col {
                Raw::Str(v) | Raw::OptStr(_, v) | Raw::I32(v) | Raw::I64(v) | Raw::Date(v) => v,
            })?;
        }
    }
    file.write_all(&m.buf)
}

/// Writes `cols` (all the same length) with the given names to `path`.
pub fn write(path: &std::path::Path, names: &[&str], cols: &[Col]) -> std::io::Result<()> {
    let nrows = match &cols[0] {
        Col::Str(v) => v.len(),
        Col::OptStr(v) => v.len(),
        Col::I64(v) => v.len(),
        Col::Date(v) => v.len(),
    };
    let plain = |v: &mut Vec<u8>, s: &[u8]| {
        v.extend_from_slice(&(s.len() as u32).to_le_bytes());
        v.extend_from_slice(s);
    };
    // Per column: (level bits, values).
    let enc: Vec<(Vec<u8>, Vec<u8>)> = cols
        .iter()
        .map(|col| {
            let (mut bits, mut vals) = (Vec::new(), Vec::new());
            match col {
                Col::Str(v) => v.iter().for_each(|s| plain(&mut vals, s)),
                Col::OptStr(v) => {
                    bits.resize(v.len().div_ceil(8), 0u8);
                    for (i, s) in v.iter().enumerate() {
                        if let Some(s) = s {
                            bits[i / 8] |= 1 << (i % 8);
                            plain(&mut vals, s);
                        }
                    }
                }
                Col::I64(v) => v.iter().for_each(|x| vals.extend_from_slice(&x.to_le_bytes())),
                Col::Date(v) => v.iter().for_each(|x| vals.extend_from_slice(&x.to_le_bytes())),
            }
            (bits, vals)
        })
        .collect();
    let raw: Vec<Raw> = cols
        .iter()
        .zip(&enc)
        .map(|(col, (bits, vals))| match col {
            Col::Str(_) => Raw::Str(vals),
            Col::OptStr(_) => Raw::OptStr(bits, vals),
            Col::I64(_) => Raw::I64(vals),
            Col::Date(_) => Raw::Date(vals),
        })
        .collect();
    write_raw(path, names, &raw, nrows)
}
