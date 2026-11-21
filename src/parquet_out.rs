//! Parquet writers for the two output tables (uncompressed, plain encoding).

use crate::URow;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_data_page_size_limit(1 << 20)
            .build(),
    )
}

fn bytes(b: &[u8]) -> ByteArray {
    ByteArray::from(b.to_vec())
}

pub fn write_user_daily(path: &Path, rows: &[URow], cats: &[Vec<u8>]) {
    let schema = Arc::new(
        parse_message_type(
            "message user_daily {
                required binary user_id (STRING);
                required int32 day (DATE);
                required int64 sessions;
                required int64 events;
                required int64 revenue_cents;
                optional binary top_category (STRING);
            }",
        )
        .unwrap(),
    );
    let file = BufWriter::with_capacity(1 << 22, File::create(path).expect("create parquet"));
    let mut w = SerializedFileWriter::new(file, schema, props()).unwrap();
    if !rows.is_empty() {
        let cat_bytes: Vec<ByteArray> = cats.iter().map(|c| bytes(c)).collect();
        let mut rg = w.next_row_group().unwrap();
        let mut col = 0;
        while let Some(mut c) = rg.next_column().unwrap() {
            match col {
                0 => {
                    let v: Vec<ByteArray> = rows.iter().map(|r| bytes(crate::unpack(r.name))).collect();
                    c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
                }
                1 => {
                    let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                    c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
                }
                2 | 3 | 4 => {
                    let v: Vec<i64> = rows
                        .iter()
                        .map(|r| match col {
                            2 => r.sessions as i64,
                            3 => r.events as i64,
                            _ => r.revenue as i64,
                        })
                        .collect();
                    c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
                _ => {
                    let defs: Vec<i16> = rows.iter().map(|r| (r.top != u16::MAX) as i16).collect();
                    let v: Vec<ByteArray> = rows
                        .iter()
                        .filter(|r| r.top != u16::MAX)
                        .map(|r| cat_bytes[r.top as usize].clone())
                        .collect();
                    c.typed::<ByteArrayType>().write_batch(&v, Some(&defs), None).unwrap();
                }
            }
            c.close().unwrap();
            col += 1;
        }
        rg.close().unwrap();
    }
    w.close().unwrap();
}

pub type CRow<'a> = (&'a [u8], &'a [u8], i32, [u64; 4]);

pub fn write_campaign_daily(path: &Path, rows: &[CRow]) {
    let schema = Arc::new(
        parse_message_type(
            "message campaign_daily {
                required binary campaign_id (STRING);
                required binary channel (STRING);
                required int32 day (DATE);
                required int64 sessions;
                required int64 purchasing_sessions;
                required int64 revenue_cents;
                required int64 assisted_sessions;
            }",
        )
        .unwrap(),
    );
    let file = BufWriter::new(File::create(path).expect("create parquet"));
    let mut w = SerializedFileWriter::new(file, schema, props()).unwrap();
    if !rows.is_empty() {
        let mut rg = w.next_row_group().unwrap();
        let mut col = 0;
        while let Some(mut c) = rg.next_column().unwrap() {
            match col {
                0 | 1 => {
                    let v: Vec<ByteArray> = rows.iter().map(|r| bytes(if col == 0 { r.0 } else { r.1 })).collect();
                    c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
                }
                2 => {
                    let v: Vec<i32> = rows.iter().map(|r| r.2).collect();
                    c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
                }
                k => {
                    let v: Vec<i64> = rows.iter().map(|r| r.3[k - 3] as i64).collect();
                    c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
            }
            c.close().unwrap();
            col += 1;
        }
        rg.close().unwrap();
    }
    w.close().unwrap();
}
