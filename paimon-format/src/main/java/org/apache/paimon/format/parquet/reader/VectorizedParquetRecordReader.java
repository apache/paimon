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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.format.parquet.type.ParquetField;
import org.apache.paimon.format.parquet.type.ParquetPrimitiveField;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;

import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.paimon.format.parquet.reader.ParquetReaderUtil.createReadableColumnVectors;

/** Record reader for parquet. */
public class VectorizedParquetRecordReader implements FileRecordReader<InternalRow> {

    private ParquetFileReader reader;

    // The capacity of vectorized batch.
    private final int batchSize;

    /**
     * The total number of rows this RecordReader will eventually read. The sum of the rows of all
     * the row groups.
     */
    private final long totalRowCount;

    /** The number of rows that have been reading, including the current in flight row group. */
    private long totalCountLoadedSoFar = 0;

    /** The number of rows that have been returned. */
    private long rowsReturned;

    /**
     * Encapsulate writable column vectors with other Parquet related info such as repetition /
     * definition levels.
     */
    private ParquetColumnVector[] columnVectors;

    private ColumnarBatch columnarBatch;

    private final Path filePath;
    private final MessageType fileSchema;
    private final List<ParquetField> fields;
    private final RowIndexGenerator rowIndexGenerator;

    private Set<ParquetField> missingColumns;
    private VersionParser.ParsedVersion writerVersion;

    public VectorizedParquetRecordReader(
            Path filePath,
            ParquetFileReader reader,
            MessageType fileSchema,
            List<ParquetField> fields,
            WritableColumnVector[] vectors,
            int batchSize)
            throws IOException {
        this.filePath = filePath;
        this.reader = reader;
        this.fileSchema = fileSchema;
        this.fields = fields;
        this.totalRowCount = reader.getFilteredRecordCount();
        this.batchSize = batchSize;
        this.rowIndexGenerator = new RowIndexGenerator();

        // fetch writer version from file metadata
        try {
            this.writerVersion = VersionParser.parse(reader.getFileMetaData().getCreatedBy());
        } catch (Exception e) {
            // Swallow any exception, if we cannot parse the version we will revert to a sequential
            // read
            // if the column is a delta byte array encoding (due to PARQUET-246).
        }
        // Check if all the required columns are present in the file.
        checkMissingColumns();
        // Initialize the columnarBatch and columnVectors,
        initBatch(vectors);
    }

    private void initBatch(WritableColumnVector[] vectors) {
        columnarBatch =
                new ColumnarBatch(
                        filePath,
                        createReadableColumnVectors(
                                fields.stream()
                                        .map(ParquetField::getType)
                                        .collect(Collectors.toList()),
                                vectors));
        columnVectors = new ParquetColumnVector[fields.size()];
        for (int i = 0; i < columnVectors.length; i++) {
            columnVectors[i] =
                    new ParquetColumnVector(
                            fields.get(i), vectors[i], batchSize, missingColumns, true);
        }
    }

    private void checkMissingColumns() throws IOException {
        missingColumns = new HashSet<>();
        for (ParquetField field : fields) {
            checkColumn(field);
        }
    }

    private void checkColumn(ParquetField field) throws IOException {
        String[] path = field.path();

        if (containsPath(fileSchema, path, 0)) {
            if (field.isPrimitive()) {
                ColumnDescriptor desc = ((ParquetPrimitiveField) field).getDescriptor();
                ColumnDescriptor fd = fileSchema.getColumnDescription(desc.getPath());
                if (!fd.equals(desc)) {
                    throw new IOException("Schema evolution not supported.");
                }
            }
        } else {
            if (field.isRequired()) {
                throw new IOException(
                        "Required column is missing in data file. Col: " + Arrays.toString(path));
            }
            missingColumns.add(field);
        }
    }

    private boolean containsPath(Type parquetType, String[] path, int depth) {
        if (path.length == depth) {
            return true;
        }
        if (parquetType instanceof GroupType) {
            GroupType parquetGroupType = parquetType.asGroupType();
            String fieldName = path[depth];
            if (parquetGroupType.containsField(fieldName)) {
                return containsPath(parquetGroupType.getType(fieldName), path, depth + 1);
            }
        }
        return false;
    }

    public boolean nextBatch() throws IOException {
        try {
            if (rowsReturned >= totalRowCount) {
                return false;
            }
            for (ParquetColumnVector vector : columnVectors) {
                vector.reset();
            }
            columnarBatch.setNumRows(0);
            checkEndOfRowGroup();

            int num = (int) Math.min(batchSize, totalCountLoadedSoFar - rowsReturned);
            for (ParquetColumnVector cv : columnVectors) {
                for (ParquetColumnVector leafCv : cv.getLeaves()) {
                    VectorizedColumnReader columnReader = leafCv.getColumnReader();
                    if (columnReader != null) {
                        columnReader.readBatch(
                                num,
                                leafCv.getColumn().getType(),
                                leafCv.getValueVector(),
                                leafCv.getRepetitionLevelVector(),
                                leafCv.getDefinitionLevelVector());
                    }
                }
                cv.assemble();
            }
            rowsReturned += num;
            columnarBatch.setNumRows(num);
            rowIndexGenerator.populateRowIndex(columnarBatch);
            return true;
        } catch (IOException e) {
            throw new IOException(
                    format(
                            "Exception in nextBatch, filePath: %s fileSchema: %s",
                            filePath, fileSchema),
                    e);
        } catch (Exception e) {
            throw new RuntimeException(
                    format(
                            "Exception in nextBatch, filePath: %s fileSchema: %s",
                            filePath, fileSchema),
                    e);
        }
    }

    private void checkEndOfRowGroup() throws IOException {
        if (rowsReturned != totalCountLoadedSoFar) {
            return;
        }
        PageReadStore pages = reader.readNextFilteredRowGroup();
        if (pages == null) {
            throw new IOException(
                    "expecting more rows but reached last block. Read "
                            + rowsReturned
                            + " out of "
                            + totalRowCount);
        }

        rowIndexGenerator.initFromPageReadStore(pages);
        for (ParquetColumnVector cv : columnVectors) {
            initColumnReader(pages, cv);
        }
        totalCountLoadedSoFar += pages.getRowCount();
    }

    private void initColumnReader(PageReadStore pages, ParquetColumnVector cv) throws IOException {
        if (!missingColumns.contains(cv.getColumn())) {
            if (cv.getColumn().isPrimitive()) {
                ParquetField column = cv.getColumn();
                VectorizedColumnReader reader =
                        new VectorizedColumnReader(
                                ((ParquetPrimitiveField) column).getDescriptor(),
                                column.isRequired(),
                                pages,
                                writerVersion);
                cv.setColumnReader(reader);
            } else {
                // Not in missing columns and is a complex type: this must be a struct
                for (ParquetColumnVector childCv : cv.getChildren()) {
                    initColumnReader(pages, childCv);
                }
            }
        }
    }

    @Override
    public @Nullable FileRecordIterator<InternalRow> readBatch() throws IOException {
        if (nextBatch()) {
            return columnarBatch.vectorizedRowIterator;
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
}
