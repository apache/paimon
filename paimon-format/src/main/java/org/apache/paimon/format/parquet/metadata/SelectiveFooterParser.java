/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.parquet.metadata;

import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DateType;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.EnumType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.JsonType;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.ListType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.MapType;
import org.apache.parquet.format.NullType;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.SizeStatistics;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.TimeType;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.format.TimestampType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.TypeDefinedOrder;
import org.apache.parquet.format.UUIDType;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Selective parser of the parquet thrift footer that materializes only the requested column chunks'
 * metadata, optionally skipping their Statistics.
 *
 * <p>Instead of materializing the full {@code org.apache.parquet.format.FileMetaData} tree with the
 * generated thrift reader (which parses <b>every</b> column chunk's <b>every</b> field, including
 * all {@code Statistics}), this parser walks the compact-protocol bytes itself with {@link
 * ThriftCompactReader} and:
 *
 * <ul>
 *   <li>fully materializes only column chunks whose top-level column is requested; other column
 *       chunks are still scanned (the compact protocol forces a full byte pass) but their bytes are
 *       not retained — the transient shell and the pre-{@code path_in_schema} fields (type,
 *       encodings) are parsed and then discarded, while every post-path field is skipped;
 *   <li>materializes {@code Statistics}, {@code encoding_stats} and {@code size_statistics} only
 *       for those kept chunks;
 *   <li>materializes the schema (including the full {@code LogicalType} union) by hand, so it is
 *       always complete regardless of the column subset.
 * </ul>
 *
 * <p>Object construction (thrift structs → {@code ParquetMetadata}) is delegated to {@link
 * ParquetMetadataConverter#fromParquetMetadata(FileMetaData, InternalFileDecryptor, boolean, Map)}
 * to reuse the battle-tested conversion, so the result is byte-for-byte equivalent to the official
 * reader for the retained subset.
 *
 * <p>Encrypted footers and encrypted columns are not supported and signal {@link
 * FallbackSignalException} so the caller can retry with the official parser.
 */
public final class SelectiveFooterParser {

    /** ParquetReadOptions property carrying the comma-joined requested top-level column names. */
    public static final String REQUESTED_COLUMNS_KEY = "paimon.parquet.footer.requested.columns";

    /**
     * ParquetReadOptions property ("true"/"false") controlling whether Statistics of the
     * <i>requested</i> columns are parsed. The only consumer of footer statistics is row-group
     * filtering with a pushdown filter, which never runs together with this parser — hence AI
     * training reads (no pushdown) default to skipping them.
     */
    public static final String SKIP_STATISTICS_KEY = "paimon.parquet.footer.skip.statistics";

    /**
     * Separator character used to join column names in {@link #REQUESTED_COLUMNS_KEY}. Uses null
     * character to support column names containing commas.
     */
    public static final String COLUMN_NAME_SEPARATOR = "\0";

    /** Raised when the footer needs the official (non-selective) parse path. */
    public static final class FallbackSignalException extends RuntimeException {
        FallbackSignalException(String reason) {
            super(reason);
        }
    }

    private final ThriftCompactReader in;
    private final @Nullable Set<String> requestedColumns;
    private final boolean includeStatistics;
    private final ParquetMetadataConverter converter;

    private SelectiveFooterParser(
            byte[] footer,
            int offset,
            int length,
            @Nullable Set<String> requestedColumns,
            boolean includeStatistics,
            ParquetMetadataConverter converter) {
        this.in = new ThriftCompactReader(footer, offset, offset + length);
        this.requestedColumns = requestedColumns;
        this.includeStatistics = includeStatistics;
        this.converter = converter;
    }

    /**
     * Parses the footer bytes, materializing Statistics of the requested columns.
     *
     * @param footerBytes raw footer bytes (thrift compact serialized FileMetaData); array-backed
     *     buffers are read in place without copying
     * @param requestedColumns top-level column names to keep; {@code null} parses everything
     *     (equivalent to NO_FILTER, used to validate equivalence)
     * @param converter the caller's converter, reused for final object construction
     */
    public static ParquetMetadata parse(
            ByteBuffer footerBytes,
            @Nullable Set<String> requestedColumns,
            ParquetMetadataConverter converter)
            throws IOException {
        return parse(footerBytes, requestedColumns, true, converter);
    }

    /**
     * Parses the footer bytes.
     *
     * @param includeStatistics whether Statistics of <i>kept</i> columns are materialized; {@code
     *     false} skips them (AI training reads never consume footer statistics: the only consumer,
     *     row-group stats filtering, requires a pushdown filter, and selective parsing is enabled
     *     only when no filter is present)
     */
    public static ParquetMetadata parse(
            ByteBuffer footerBytes,
            @Nullable Set<String> requestedColumns,
            boolean includeStatistics,
            ParquetMetadataConverter converter)
            throws IOException {
        if (requestedColumns != null && requestedColumns.isEmpty()) {
            // nothing can be retained: every column chunk would be dropped and the parse would
            // end in FallbackSignalException after a full scan — fail fast instead
            throw new FallbackSignalException("empty requested columns");
        }
        byte[] bytes;
        int offset;
        int length = footerBytes.remaining();
        if (footerBytes.hasArray()) {
            bytes = footerBytes.array();
            offset = footerBytes.arrayOffset() + footerBytes.position();
        } else {
            bytes = new byte[length];
            footerBytes.duplicate().get(bytes);
            offset = 0;
        }
        SelectiveFooterParser parser =
                new SelectiveFooterParser(
                        bytes, offset, length, requestedColumns, includeStatistics, converter);
        try {
            return parser.parseFileMetaData();
        } catch (FallbackSignalException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            // any mismatch with the expected structure falls back to the official parser
            throw new FallbackSignalException("selective footer parse failed: " + e);
        }
    }

    // ------------------------------------------------------------------ FileMetaData

    private ParquetMetadata parseFileMetaData() throws IOException {
        FileMetaData fileMetaData = new FileMetaData();
        List<RowGroup> rowGroups = new ArrayList<>();
        Map<RowGroup, Long> rowGroupOffsets = new HashMap<>();

        in.structBegin();
        int header;
        while ((header = in.readFieldHeader()) != ThriftCompactReader.STOP) {
            int id = ThriftCompactReader.fieldId(header);
            byte type = ThriftCompactReader.type(header);
            switch (id) {
                case 1: // version
                    fileMetaData.setVersion(in.readI32());
                    break;
                case 2: // schema
                    fileMetaData.setSchema(readSchemaElements());
                    break;
                case 3: // num_rows
                    fileMetaData.setNum_rows(in.readI64());
                    break;
                case 4: // row_groups
                    readRowGroups(rowGroups, rowGroupOffsets);
                    break;
                case 5: // key_value_metadata
                    fileMetaData.setKey_value_metadata(readKeyValues());
                    break;
                case 6: // created_by
                    fileMetaData.setCreated_by(in.readString());
                    break;
                case 7: // column_orders
                    fileMetaData.setColumn_orders(readColumnOrders());
                    break;
                case 8: // encryption_algorithm (plaintext footer with encrypted columns)
                    throw new FallbackSignalException("footer carries encryption_algorithm");
                default: // 9: footer_signing_key_metadata and unknown future fields
                    in.skip(type);
            }
        }
        in.structEnd();
        fileMetaData.setRow_groups(rowGroups);
        return converter.fromParquetMetadata(fileMetaData, null, false, rowGroupOffsets);
    }

    // ------------------------------------------------------------------ schema (manual)

    private List<SchemaElement> readSchemaElements() throws IOException {
        int listHeader = in.readListHeader();
        if (ThriftCompactReader.listElemType(listHeader) != ThriftCompactReader.STRUCT) {
            throw new FallbackSignalException("schema element list of non-struct type");
        }
        int size = ThriftCompactReader.listSize(listHeader);
        List<SchemaElement> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(readSchemaElement());
        }
        return result;
    }

    private SchemaElement readSchemaElement() throws IOException {
        SchemaElement element = new SchemaElement();
        in.structBegin();
        int header;
        while ((header = in.readFieldHeader()) != ThriftCompactReader.STOP) {
            int id = ThriftCompactReader.fieldId(header);
            byte type = ThriftCompactReader.type(header);
            switch (id) {
                case 1: // type (enum)
                    element.setType(Type.findByValue(in.readI32()));
                    break;
                case 2: // type_length
                    element.setType_length(in.readI32());
                    break;
                case 3: // repetition_type (enum)
                    element.setRepetition_type(FieldRepetitionType.findByValue(in.readI32()));
                    break;
                case 4: // name
                    element.setName(in.readString());
                    break;
                case 5: // num_children
                    element.setNum_children(in.readI32());
                    break;
                case 6: // converted_type (enum)
                    element.setConverted_type(ConvertedType.findByValue(in.readI32()));
                    break;
                case 7: // scale
                    element.setScale(in.readI32());
                    break;
                case 8: // precision
                    element.setPrecision(in.readI32());
                    break;
                case 9: // field_id
                    element.setField_id(in.readI32());
                    break;
                case 10: // logicalType (union)
                    element.setLogicalType(readLogicalType());
                    break;
                default:
                    // unknown future fields are skipped, matching the official generated reader
                    in.skip(type);
            }
        }
        in.structEnd();
        return element;
    }

    /**
     * Reads a LogicalType union. Exactly one member is set; members are mostly empty structs, so
     * each case just consumes the inner struct. Unknown members are skipped (the official generated
     * reader would skip them too), keeping the two parsers behavior-identical.
     */
    private LogicalType readLogicalType() throws IOException {
        LogicalType logicalType = new LogicalType();
        in.structBegin();
        int header;
        while ((header = in.readFieldHeader()) != ThriftCompactReader.STOP) {
            int id = ThriftCompactReader.fieldId(header);
            switch (id) {
                case 1: // STRING
                    skipEmptyStruct();
                    logicalType.setSTRING(new StringType());
                    break;
                case 2: // MAP
                    skipEmptyStruct();
                    logicalType.setMAP(new MapType());
                    break;
                case 3: // LIST
                    skipEmptyStruct();
                    logicalType.setLIST(new ListType());
                    break;
                case 4: // ENUM
                    skipEmptyStruct();
                    logicalType.setENUM(new EnumType());
                    break;
                case 5: // DECIMAL {1 scale, 2 precision}
                    {
                        DecimalType decimal = new DecimalType();
                        in.structBegin();
                        int inner;
                        while ((inner = in.readFieldHeader()) != ThriftCompactReader.STOP) {
                            switch (ThriftCompactReader.fieldId(inner)) {
                                case 1:
                                    decimal.setScale(in.readI32());
                                    break;
                                case 2:
                                    decimal.setPrecision(in.readI32());
                                    break;
                                default:
                                    in.skip(ThriftCompactReader.type(inner));
                            }
                        }
                        in.structEnd();
                        logicalType.setDECIMAL(decimal);
                        break;
                    }
                case 6: // DATE
                    skipEmptyStruct();
                    logicalType.setDATE(new DateType());
                    break;
                case 7: // TIME {1 isAdjustedToUTC (bool header), 2 unit TimeUnit}
                    {
                        TimeType time = new TimeType();
                        readTimeUnitStruct(time, null);
                        logicalType.setTIME(time);
                        break;
                    }
                case 8: // TIMESTAMP {1 isAdjustedToUTC, 2 unit}
                    {
                        TimestampType timestamp = new TimestampType();
                        readTimeUnitStruct(null, timestamp);
                        logicalType.setTIMESTAMP(timestamp);
                        break;
                    }
                case 10: // INTEGER {1 bitWidth (byte), 2 isSigned (bool)}
                    {
                        IntType intType = new IntType();
                        in.structBegin();
                        int inner;
                        while ((inner = in.readFieldHeader()) != ThriftCompactReader.STOP) {
                            byte innerType = ThriftCompactReader.type(inner);
                            switch (ThriftCompactReader.fieldId(inner)) {
                                case 1:
                                    // i8 values are a single raw byte in the compact protocol
                                    intType.setBitWidth(in.readByteValue());
                                    break;
                                case 2:
                                    intType.setIsSigned(
                                            innerType == ThriftCompactReader.BOOLEAN_TRUE);
                                    break;
                                default:
                                    in.skip(innerType);
                            }
                        }
                        in.structEnd();
                        logicalType.setINTEGER(intType);
                        break;
                    }
                case 11: // UNKNOWN
                    skipEmptyStruct();
                    logicalType.setUNKNOWN(new NullType());
                    break;
                case 12: // JSON
                    skipEmptyStruct();
                    logicalType.setJSON(new JsonType());
                    break;
                case 13: // BSON
                    skipEmptyStruct();
                    logicalType.setBSON(new org.apache.parquet.format.BsonType());
                    break;
                case 14: // UUID
                    skipEmptyStruct();
                    logicalType.setUUID(new UUIDType());
                    break;
                case 15: // FLOAT16
                    skipEmptyStruct();
                    logicalType.setFLOAT16(new org.apache.parquet.format.Float16Type());
                    break;
                default:
                    in.skip(ThriftCompactReader.type(header));
            }
        }
        in.structEnd();
        return logicalType;
    }

    /** Reads TIME or TIMESTAMP {1 isAdjustedToUTC (bool), 2 unit (TimeUnit union)}. */
    private void readTimeUnitStruct(@Nullable TimeType time, @Nullable TimestampType timestamp)
            throws IOException {
        in.structBegin();
        int header;
        while ((header = in.readFieldHeader()) != ThriftCompactReader.STOP) {
            int id = ThriftCompactReader.fieldId(header);
            byte type = ThriftCompactReader.type(header);
            if (id == 1) { // isAdjustedToUTC
                boolean value = type == ThriftCompactReader.BOOLEAN_TRUE;
                if (time != null) {
                    time.setIsAdjustedToUTC(value);
                } else {
                    timestamp.setIsAdjustedToUTC(value);
                }
            } else if (id == 2) { // unit
                TimeUnit unit = new TimeUnit();
                in.structBegin();
                int inner;
                while ((inner = in.readFieldHeader()) != ThriftCompactReader.STOP) {
                    switch (ThriftCompactReader.fieldId(inner)) {
                        case 1: // MILLIS
                            skipEmptyStruct();
                            unit.setMILLIS(new org.apache.parquet.format.MilliSeconds());
                            break;
                        case 2: // MICROS
                            skipEmptyStruct();
                            unit.setMICROS(new org.apache.parquet.format.MicroSeconds());
                            break;
                        case 3: // NANOS
                            skipEmptyStruct();
                            unit.setNANOS(new org.apache.parquet.format.NanoSeconds());
                            break;
                        default:
                            in.skip(ThriftCompactReader.type(inner));
                    }
                }
                in.structEnd();
                if (time != null) {
                    time.setUnit(unit);
                } else {
                    timestamp.setUnit(unit);
                }
            } else {
                in.skip(type);
            }
        }
        in.structEnd();
    }

    private void skipEmptyStruct() throws IOException {
        in.structBegin();
        int inner;
        while ((inner = in.readFieldHeader()) != ThriftCompactReader.STOP) {
            in.skip(ThriftCompactReader.type(inner));
        }
        in.structEnd();
    }

    // ------------------------------------------------------------------ row groups

    private void readRowGroups(List<RowGroup> rowGroups, Map<RowGroup, Long> rowGroupOffsets)
            throws IOException {
        int listHeader = in.readListHeader();
        if (ThriftCompactReader.listElemType(listHeader) != ThriftCompactReader.STRUCT) {
            throw new FallbackSignalException("row group list of non-struct type");
        }
        int size = ThriftCompactReader.listSize(listHeader);
        long rowIdxSum = 0;
        for (int i = 0; i < size; i++) {
            RowGroup rowGroup = new RowGroup();
            List<ColumnChunk> columns = new ArrayList<>();
            in.structBegin();
            int header;
            while ((header = in.readFieldHeader()) != ThriftCompactReader.STOP) {
                int id = ThriftCompactReader.fieldId(header);
                byte type = ThriftCompactReader.type(header);
                switch (id) {
                    case 1: // columns
                        readColumnChunks(columns);
                        break;
                    case 2: // total_byte_size
                        rowGroup.setTotal_byte_size(in.readI64());
                        break;
                    case 3: // num_rows
                        rowGroup.setNum_rows(in.readI64());
                        break;
                    case 4: // sorting_columns (not consumed by fromParquetMetadata)
                        in.skip(type);
                        break;
                    case 5: // file_offset
                        rowGroup.setFile_offset(in.readI64());
                        break;
                    case 6: // total_compressed_size
                        rowGroup.setTotal_compressed_size(in.readI64());
                        break;
                    case 7: // ordinal
                        rowGroup.setOrdinal(in.readI16());
                        break;
                    default:
                        in.skip(type);
                }
            }
            in.structEnd();
            // mirror generateRowGroupOffsets: offset of this row group is the rows before it
            rowGroup.setColumns(columns);
            rowGroupOffsets.put(rowGroup, rowIdxSum);
            rowIdxSum += rowGroup.getNum_rows();
            rowGroups.add(rowGroup);
        }
    }

    // ------------------------------------------------------------------ column chunks

    private void readColumnChunks(List<ColumnChunk> columns) throws IOException {
        int listHeader = in.readListHeader();
        if (ThriftCompactReader.listElemType(listHeader) != ThriftCompactReader.STRUCT) {
            throw new FallbackSignalException("column chunk list of non-struct type");
        }
        int size = ThriftCompactReader.listSize(listHeader);
        for (int i = 0; i < size; i++) {
            ColumnChunk chunk = new ColumnChunk();
            ColumnMetaData meta = null;
            in.structBegin();
            int header;
            while ((header = in.readFieldHeader()) != ThriftCompactReader.STOP) {
                int id = ThriftCompactReader.fieldId(header);
                byte type = ThriftCompactReader.type(header);
                switch (id) {
                    case 1: // file_path
                        chunk.setFile_path(in.readString());
                        break;
                    case 2: // file_offset
                        chunk.setFile_offset(in.readI64());
                        break;
                    case 3: // meta_data
                        meta = readColumnMetaData();
                        break;
                    case 4: // offset_index_offset
                        chunk.setOffset_index_offset(in.readI64());
                        break;
                    case 5: // offset_index_length
                        chunk.setOffset_index_length(in.readI32());
                        break;
                    case 6: // column_index_offset
                        chunk.setColumn_index_offset(in.readI64());
                        break;
                    case 7: // column_index_length
                        chunk.setColumn_index_length(in.readI32());
                        break;
                    case 8: // crypto_metadata
                    case 9: // encrypted_column_metadata
                        throw new FallbackSignalException("encrypted column chunk metadata");
                    default:
                        in.skip(type);
                }
            }
            in.structEnd();
            if (meta != null) {
                chunk.setMeta_data(meta);
                columns.add(chunk);
            }
            // meta == null: column not requested — the chunk is not retained; its bytes were
            // still scanned, and the transient shell plus pre-path fields (file_path, type,
            // encodings) were parsed and then discarded here.
        }
        if (requestedColumns != null && columns.isEmpty()) {
            // Every chunk was dropped: none of the requested top-level columns exists in this
            // file (schema evolution). fromParquetMetadata cannot build an empty block
            // (it reads columns.get(0).file_path), so fall back to the official parser.
            throw new FallbackSignalException("no requested column present in a row group");
        }
    }

    /**
     * Reads one ColumnMetaData struct. Returns {@code null} (after consuming all bytes) when the
     * column is not requested; returns the fully materialized struct otherwise.
     */
    private ColumnMetaData readColumnMetaData() throws IOException {
        ColumnMetaData meta = new ColumnMetaData();
        // null = path not seen yet, TRUE = keep, FALSE = drop
        Boolean keep = requestedColumns == null ? Boolean.TRUE : null;
        in.structBegin();
        int header;
        while ((header = in.readFieldHeader()) != ThriftCompactReader.STOP) {
            int id = ThriftCompactReader.fieldId(header);
            byte type = ThriftCompactReader.type(header);
            switch (id) {
                case 1: // type (enum)
                    if (shouldMaterialize(keep)) {
                        meta.setType(org.apache.parquet.format.Type.findByValue(in.readI32()));
                    } else {
                        in.skip(type);
                    }
                    break;
                case 2: // encodings
                    {
                        if (shouldMaterialize(keep)) {
                            meta.setEncodings(readEncodings());
                        } else {
                            in.skip(type);
                        }
                        break;
                    }
                case 3: // path_in_schema
                    {
                        List<String> path = readStringList();
                        if (keep == null) {
                            keep = isRequested(path);
                        }
                        if (shouldMaterialize(keep)) {
                            meta.setPath_in_schema(path);
                        }
                        break;
                    }
                case 4: // codec (enum)
                    if (shouldMaterialize(keep)) {
                        meta.setCodec(CompressionCodec.findByValue(in.readI32()));
                    } else {
                        in.skip(type);
                    }
                    break;
                case 5: // num_values
                    if (shouldMaterialize(keep)) {
                        meta.setNum_values(in.readI64());
                    } else {
                        in.skip(type);
                    }
                    break;
                case 6: // total_uncompressed_size
                    if (shouldMaterialize(keep)) {
                        meta.setTotal_uncompressed_size(in.readI64());
                    } else {
                        in.skip(type);
                    }
                    break;
                case 7: // total_compressed_size
                    if (shouldMaterialize(keep)) {
                        meta.setTotal_compressed_size(in.readI64());
                    } else {
                        in.skip(type);
                    }
                    break;
                case 8: // key_value_metadata (unused by fromParquetMetadata)
                    in.skip(type);
                    break;
                case 9: // data_page_offset
                    if (shouldMaterialize(keep)) {
                        meta.setData_page_offset(in.readI64());
                    } else {
                        in.skip(type);
                    }
                    break;
                case 10: // index_page_offset
                    if (shouldMaterialize(keep)) {
                        meta.setIndex_page_offset(in.readI64());
                    } else {
                        in.skip(type);
                    }
                    break;
                case 11: // dictionary_page_offset
                    if (shouldMaterialize(keep)) {
                        meta.setDictionary_page_offset(in.readI64());
                    } else {
                        in.skip(type);
                    }
                    break;
                case 12: // statistics
                    if (shouldMaterialize(keep) && includeStatistics) {
                        meta.setStatistics(readStatistics());
                    } else {
                        in.skip(type);
                    }
                    break;
                case 13: // encoding_stats
                    if (shouldMaterialize(keep)) {
                        meta.setEncoding_stats(readPageEncodingStats());
                    } else {
                        in.skip(type);
                    }
                    break;
                case 14: // bloom_filter_offset
                    if (shouldMaterialize(keep)) {
                        meta.setBloom_filter_offset(in.readI64());
                    } else {
                        in.skip(type);
                    }
                    break;
                case 15: // bloom_filter_length
                    if (shouldMaterialize(keep)) {
                        meta.setBloom_filter_length(in.readI32());
                    } else {
                        in.skip(type);
                    }
                    break;
                case 16: // size_statistics
                    if (shouldMaterialize(keep)) {
                        meta.setSize_statistics(readSizeStatistics());
                    } else {
                        in.skip(type);
                    }
                    break;
                default:
                    in.skip(type);
            }
        }
        in.structEnd();
        return shouldMaterialize(keep) ? meta : null;
    }

    private boolean shouldMaterialize(Boolean keep) {
        return keep == null || keep;
    }

    private boolean isRequested(List<String> path) {
        if (path.isEmpty()) {
            // An empty path is corrupt data. Keep the chunk so the official
            // fromParquetMetadata handling applies (it throws on the empty path and the parse
            // falls back there), instead of silently dropping it like the official reader never
            // would.
            return true;
        }
        return requestedColumns.contains(path.get(0));
    }

    // ------------------------------------------------------------------ small structs

    private List<org.apache.parquet.format.Encoding> readEncodings() throws IOException {
        int listHeader = in.readListHeader();
        if (ThriftCompactReader.listElemType(listHeader) != ThriftCompactReader.I32) {
            throw new FallbackSignalException("encoding list of non-i32 type");
        }
        int size = ThriftCompactReader.listSize(listHeader);
        List<Encoding> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(Encoding.findByValue(in.readI32()));
        }
        return result;
    }

    private List<String> readStringList() throws IOException {
        int listHeader = in.readListHeader();
        if (ThriftCompactReader.listElemType(listHeader) != ThriftCompactReader.BINARY) {
            throw new FallbackSignalException("string list of non-binary type");
        }
        int size = ThriftCompactReader.listSize(listHeader);
        List<String> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(in.readString());
        }
        return result;
    }

    private List<KeyValue> readKeyValues() throws IOException {
        int listHeader = in.readListHeader();
        if (ThriftCompactReader.listElemType(listHeader) != ThriftCompactReader.STRUCT) {
            throw new FallbackSignalException("key value list of non-struct type");
        }
        int size = ThriftCompactReader.listSize(listHeader);
        List<KeyValue> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            KeyValue kv = new KeyValue();
            in.structBegin();
            int header;
            while ((header = in.readFieldHeader()) != ThriftCompactReader.STOP) {
                switch (ThriftCompactReader.fieldId(header)) {
                    case 1:
                        kv.setKey(in.readString());
                        break;
                    case 2:
                        kv.setValue(in.readString());
                        break;
                    default:
                        in.skip(ThriftCompactReader.type(header));
                }
            }
            in.structEnd();
            result.add(kv);
        }
        return result;
    }

    private List<ColumnOrder> readColumnOrders() throws IOException {
        int listHeader = in.readListHeader();
        if (ThriftCompactReader.listElemType(listHeader) != ThriftCompactReader.STRUCT) {
            throw new FallbackSignalException("column order list of non-struct type");
        }
        int size = ThriftCompactReader.listSize(listHeader);
        List<ColumnOrder> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ColumnOrder order = new ColumnOrder();
            in.structBegin();
            int header;
            while ((header = in.readFieldHeader()) != ThriftCompactReader.STOP) {
                if (ThriftCompactReader.fieldId(header) == 1) { // TYPE_ORDER
                    // consume the (empty) TypeDefinedOrder struct
                    in.structBegin();
                    int inner;
                    while ((inner = in.readFieldHeader()) != ThriftCompactReader.STOP) {
                        in.skip(ThriftCompactReader.type(inner));
                    }
                    in.structEnd();
                    order.setTYPE_ORDER(new TypeDefinedOrder());
                } else {
                    in.skip(ThriftCompactReader.type(header));
                }
            }
            in.structEnd();
            result.add(order);
        }
        return result;
    }

    private org.apache.parquet.format.Statistics readStatistics() throws IOException {
        Statistics statistics = new Statistics();
        in.structBegin();
        int header;
        while ((header = in.readFieldHeader()) != ThriftCompactReader.STOP) {
            byte type = ThriftCompactReader.type(header);
            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // max (deprecated)
                    statistics.setMax(in.readBinary());
                    break;
                case 2: // min (deprecated)
                    statistics.setMin(in.readBinary());
                    break;
                case 3: // null_count
                    statistics.setNull_count(in.readI64());
                    break;
                case 4: // distinct_count
                    statistics.setDistinct_count(in.readI64());
                    break;
                case 5: // max_value
                    statistics.setMax_value(in.readBinary());
                    break;
                case 6: // min_value
                    statistics.setMin_value(in.readBinary());
                    break;
                case 7: // is_max_value_exact (boolean value is the field header type)
                    statistics.setIs_max_value_exact(type == ThriftCompactReader.BOOLEAN_TRUE);
                    break;
                case 8: // is_min_value_exact
                    statistics.setIs_min_value_exact(type == ThriftCompactReader.BOOLEAN_TRUE);
                    break;
                default:
                    in.skip(type);
            }
        }
        in.structEnd();
        return statistics;
    }

    private List<PageEncodingStats> readPageEncodingStats() throws IOException {
        int listHeader = in.readListHeader();
        if (ThriftCompactReader.listElemType(listHeader) != ThriftCompactReader.STRUCT) {
            throw new FallbackSignalException("page encoding stats list of non-struct type");
        }
        int size = ThriftCompactReader.listSize(listHeader);
        List<PageEncodingStats> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            PageEncodingStats stats = new PageEncodingStats();
            in.structBegin();
            int header;
            while ((header = in.readFieldHeader()) != ThriftCompactReader.STOP) {
                switch (ThriftCompactReader.fieldId(header)) {
                    case 1: // page_type (enum)
                        stats.setPage_type(
                                org.apache.parquet.format.PageType.findByValue(in.readI32()));
                        break;
                    case 2: // encoding (enum)
                        stats.setEncoding(Encoding.findByValue(in.readI32()));
                        break;
                    case 3: // count
                        stats.setCount(in.readI32());
                        break;
                    default:
                        in.skip(ThriftCompactReader.type(header));
                }
            }
            in.structEnd();
            result.add(stats);
        }
        return result;
    }

    private SizeStatistics readSizeStatistics() throws IOException {
        SizeStatistics statistics = new SizeStatistics();
        in.structBegin();
        int header;
        while ((header = in.readFieldHeader()) != ThriftCompactReader.STOP) {
            byte type = ThriftCompactReader.type(header);
            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // unencoded_byte_array_data_bytes
                    statistics.setUnencoded_byte_array_data_bytes(in.readI64());
                    break;
                case 2: // repetition_level_histogram
                    statistics.setRepetition_level_histogram(readI64List());
                    break;
                case 3: // definition_level_histogram
                    statistics.setDefinition_level_histogram(readI64List());
                    break;
                default:
                    in.skip(type);
            }
        }
        in.structEnd();
        return statistics;
    }

    private List<Long> readI64List() throws IOException {
        int listHeader = in.readListHeader();
        if (ThriftCompactReader.listElemType(listHeader) != ThriftCompactReader.I64) {
            throw new FallbackSignalException("histogram list of non-i64 type");
        }
        int size = ThriftCompactReader.listSize(listHeader);
        List<Long> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(in.readI64());
        }
        return result;
    }

    /** Utility for callers holding the property string from {@link #REQUESTED_COLUMNS_KEY}. */
    public static Set<String> splitRequestedColumns(String propertyValue) {
        Set<String> result = new HashSet<>();
        // Use null character as separator to support column names containing commas
        for (String name : propertyValue.split(COLUMN_NAME_SEPARATOR)) {
            String trimmed = name.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }
}
