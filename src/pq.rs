//! A minimal Parquet writer for flat tables: one row group, one PLAIN data page per
//! column, no compression, no statistics. Thrift compact protocol written by hand.

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

/// Writes `cols` (all the same length) with the given names to `path`.
pub fn write(path: &std::path::Path, names: &[&str], cols: &[Col]) -> std::io::Result<()> {
    let nrows = match &cols[0] {
        Col::Str(v) => v.len(),
        Col::OptStr(v) => v.len(),
        Col::I64(v) => v.len(),
        Col::Date(v) => v.len(),
    };
    let est: usize = cols
        .iter()
        .map(|c| match c {
            Col::Str(v) => v.iter().map(|s| s.len() + 4).sum::<usize>(),
            Col::OptStr(v) => v.len() / 8 + v.iter().flatten().map(|s| s.len() + 4).sum::<usize>(),
            Col::I64(v) => v.len() * 8,
            Col::Date(v) => v.len() * 4,
        })
        .sum();
    let mut out: Vec<u8> = Vec::with_capacity(est + 4096);
    out.extend_from_slice(b"PAR1");
    let mut chunks = Vec::new(); // (type, data_page_offset, total size)
    let mut page = Vec::new();
    for col in cols.iter().filter(|_| nrows > 0) {
        page.clear();
        let ty = match col {
            Col::Str(v) => {
                let total: usize = v.iter().map(|s| s.len() + 4).sum();
                page.reserve(total);
                for s in v {
                    page.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    page.extend_from_slice(s);
                }
                TYPE_BYTE_ARRAY
            }
            Col::OptStr(v) => {
                // Definition levels: RLE/bit-packed hybrid, one bit-packed run, 4-byte length prefix.
                let groups = v.len().div_ceil(8);
                let mut levels = Vec::with_capacity(groups + 10);
                let mut h = Thrift::new();
                h.varint(((groups as u64) << 1) | 1);
                levels.extend_from_slice(&h.buf);
                for g in 0..groups {
                    let mut byte = 0u8;
                    for k in 0..8 {
                        if let Some(Some(_)) = v.get(g * 8 + k) {
                            byte |= 1 << k;
                        }
                    }
                    levels.push(byte);
                }
                page.extend_from_slice(&(levels.len() as u32).to_le_bytes());
                page.extend_from_slice(&levels);
                for s in v.iter().flatten() {
                    page.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    page.extend_from_slice(s);
                }
                TYPE_BYTE_ARRAY
            }
            Col::I64(v) => {
                page.reserve(v.len() * 8);
                for x in v {
                    page.extend_from_slice(&x.to_le_bytes());
                }
                TYPE_INT64
            }
            Col::Date(v) => {
                page.reserve(v.len() * 4);
                for x in v {
                    page.extend_from_slice(&x.to_le_bytes());
                }
                TYPE_INT32
            }
        };
        // PageHeader
        let mut h = Thrift::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, page.len() as i32);
        h.i32(3, page.len() as i32);
        h.begin_struct(5); // DataPageHeader
        h.i32(1, nrows as i32);
        h.i32(2, 0); // PLAIN
        h.i32(3, 3); // RLE
        h.i32(4, 3); // RLE
        h.end_struct();
        h.buf.push(0);
        let offset = out.len();
        out.extend_from_slice(&h.buf);
        out.extend_from_slice(&page);
        chunks.push((ty, offset, out.len() - offset));
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
            Col::Str(_) => (TYPE_BYTE_ARRAY, 0, 0, 1),
            Col::OptStr(_) => (TYPE_BYTE_ARRAY, 1, 0, 1),
            Col::I64(_) => (TYPE_INT64, 0, -1, 0),
            Col::Date(_) => (TYPE_INT32, 0, 6, 6),
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
        m.bin(6, b"sessions");
        m.buf.push(0);
        out.extend_from_slice(&m.buf);
        out.extend_from_slice(&(m.buf.len() as u32).to_le_bytes());
        out.extend_from_slice(b"PAR1");
        return std::fs::File::create(path)?.write_all(&out);
    }
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
    m.bin(6, b"sessions");
    m.buf.push(0);
    out.extend_from_slice(&m.buf);
    out.extend_from_slice(&(m.buf.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    std::fs::File::create(path)?.write_all(&out)
}
