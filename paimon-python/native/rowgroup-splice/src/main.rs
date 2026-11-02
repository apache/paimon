// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use parquet::bloom_filter::Sbbf;
use parquet::column::writer::ColumnCloseResult;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{
    ColumnChunkMetaData, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::path::Path;
use std::sync::Arc;

fn invalid(message: impl Into<String>) -> ParquetError {
    ParquetError::General(message.into())
}

fn metadata(file: &File) -> Result<ParquetMetaData> {
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(file)
}

fn bloom_filter(column: &ColumnChunkMetaData, input: &File) -> Result<Option<Sbbf>> {
    if column.bloom_filter_offset().is_none() {
        return Ok(None);
    }
    Sbbf::read_from_column_chunk(column, input)?
        .ok_or_else(|| invalid("bloom filter declared but unreadable"))
        .map(Some)
}

fn splice(
    source_path: &Path,
    output_path: &Path,
    replacements: &BTreeMap<usize, String>,
) -> Result<()> {
    let source = File::open(source_path)?;
    let source_meta = metadata(&source)?;
    let schema = source_meta.file_metadata().schema_descr().root_schema_ptr();
    let source_rows = source_meta.row_groups();
    if replacements.is_empty() {
        return Err(invalid("at least one replacement is required"));
    }
    if replacements.keys().any(|index| *index >= source_rows.len()) {
        return Err(invalid("replacement row group is out of range"));
    }

    let mut replacement_files = BTreeMap::new();
    for (index, path) in replacements {
        let file = File::open(path)?;
        let meta = metadata(&file)?;
        if meta.file_metadata().schema_descr().root_schema() != schema.as_ref() {
            return Err(invalid(format!(
                "replacement {index} has a different Parquet schema"
            )));
        }
        if meta.num_row_groups() == 0
            || meta
                .row_groups()
                .iter()
                .map(|group| group.num_rows())
                .sum::<i64>()
                != source_rows[*index].num_rows()
        {
            return Err(invalid(format!(
                "replacement {index} has a different row count"
            )));
        }
        replacement_files.insert(*index, (file, meta));
    }

    let output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_path)?;
    let result = (|| {
        let properties = WriterProperties::builder()
            .set_key_value_metadata(source_meta.file_metadata().key_value_metadata().cloned())
            .build();
        let mut writer = SerializedFileWriter::new(output, schema, Arc::new(properties))?;
        for (rg_index, _) in source_rows.iter().enumerate() {
            let (input, meta, group_indexes): (&File, &ParquetMetaData, Vec<usize>) =
                match replacement_files.get(&rg_index) {
                    Some((file, meta)) => (file, meta, (0..meta.num_row_groups()).collect()),
                    None => (&source, &source_meta, vec![rg_index]),
                };
            for source_index in group_indexes {
                let row_group = meta.row_group(source_index);
                let column_indexes = meta.column_index().and_then(|all| all.get(source_index));
                let offset_indexes = meta.offset_index().and_then(|all| all.get(source_index));
                let mut out_group = writer.next_row_group()?;
                for (col_index, column) in row_group.columns().iter().enumerate() {
                    let result = ColumnCloseResult {
                        bytes_written: column.compressed_size() as _,
                        rows_written: row_group.num_rows() as _,
                        metadata: column.clone(),
                        bloom_filter: bloom_filter(column, input)?,
                        column_index: column_indexes.and_then(|row| row.get(col_index)).cloned(),
                        offset_index: offset_indexes.and_then(|row| row.get(col_index)).cloned(),
                    };
                    out_group.append_column(input, result)?;
                }
                out_group.close()?;
            }
        }
        writer.close()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(output_path);
    }
    result
}

fn main() {
    let run = || -> Result<()> {
        let args: Vec<String> = std::env::args().collect();
        if args.len() < 4 || (args.len() - 3) % 2 != 0 {
            return Err(invalid("usage: paimon-rowgroup-splice SOURCE OUTPUT INDEX REPLACEMENT [INDEX REPLACEMENT ...]"));
        }
        let mut replacements = BTreeMap::new();
        for pair in args[3..].chunks_exact(2) {
            let index = pair[0]
                .parse::<usize>()
                .map_err(|_| invalid("invalid row group index"))?;
            if replacements.insert(index, pair[1].clone()).is_some() {
                return Err(invalid("duplicate row group index"));
            }
        }
        splice(Path::new(&args[1]), Path::new(&args[2]), &replacements)
    };
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
