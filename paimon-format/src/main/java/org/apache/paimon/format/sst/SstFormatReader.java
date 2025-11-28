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

package org.apache.paimon.format.sst;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.format.sst.layout.BlockEntry;
import org.apache.paimon.format.sst.layout.BlockIterator;
import org.apache.paimon.format.sst.layout.SstFileReader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** The FormatReader for SST Files. */
public class SstFormatReader implements FileRecordReader<InternalRow> {
    private final RowCompactedSerializer keySerializer;
    private final RowCompactedSerializer valueSerializer;

    /**
     * Use {@link DataEvolutionRow} to combine key row and value row as well as doing projection.
     */
    private final DataEvolutionRow resultRow;

    private final SstFileReader fileReader;
    private final SeekableInputStream inputStream;
    private final Path filePath;
    @Nullable private final List<Integer> selection;
    int nextSelection = 0;
    int returnedPosition = -1;

    public SstFormatReader(
            FileIO fileIO,
            Path filePath,
            long fileSize,
            @Nullable List<Integer> selection,
            RowType projectedRowType,
            RowType keyType,
            RowType valueType)
            throws IOException {
        this.keySerializer = new RowCompactedSerializer(keyType);
        this.valueSerializer = new RowCompactedSerializer(valueType);
        this.resultRow = createResultRow(projectedRowType, keyType, valueType);
        this.inputStream = fileIO.newInputStream(filePath);
        this.filePath = filePath;
        this.fileReader =
                new SstFileReader(
                        inputStream, keySerializer.createSliceComparator(), fileSize, filePath);
        this.selection = selection;
    }

    private DataEvolutionRow createResultRow(RowType dataType, RowType keyType, RowType valueType) {
        final int keyRowInd = 0, valueRowInd = 1;
        int[] rowOffsets = new int[dataType.getFieldCount()];
        int[] fieldOffsets = new int[dataType.getFieldCount()];

        List<DataField> dataFields = dataType.getFields();
        for (int i = 0; i < dataFields.size(); i++) {
            String fieldName = dataFields.get(i).name();
            int fieldIndex = keyType.getFieldIndex(fieldName);
            if (fieldIndex >= 0) {
                rowOffsets[i] = keyRowInd;
            } else {
                fieldIndex = valueType.getFieldIndex(fieldName);
                if (fieldIndex < 0) {
                    throw new RuntimeException(
                            String.format(
                                    "Field %s is not found in key type nor value type.",
                                    fieldName));
                }
                rowOffsets[i] = valueRowInd;
            }
            fieldOffsets[i] = fieldIndex;
        }

        return new DataEvolutionRow(2, rowOffsets, fieldOffsets);
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        // if selection is not null, we need to seek to the next position
        if (selection != null) {
            if (selectionEnd()) {
                return null;
            }
            fileReader.seekTo(getNextPosition());
        }

        BlockIterator blockIterator = fileReader.readBatch();

        if (blockIterator == null) {
            return null;
        }

        final int blockStartPos = getNextPosition() - blockIterator.getRecordPosition();

        return new FileRecordIterator<InternalRow>() {
            @Override
            public long returnedPosition() {
                return returnedPosition;
            }

            @Override
            public Path filePath() {
                return filePath;
            }

            @Nullable
            @Override
            public InternalRow next() {
                if (!blockIterator.hasNext() || selectionEnd()) {
                    return null;
                }

                int nextPosition = getNextPosition();
                if (selection != null) {
                    // if next position is in another data block, return null immediately
                    if (nextPosition >= blockStartPos + blockIterator.getRecordCount()) {
                        return null;
                    }
                    // else we need to seek to data block inner position
                    blockIterator.seekTo(nextPosition - blockStartPos);
                    nextSelection++;
                }
                returnedPosition = nextPosition;

                return readNext();
            }

            private InternalRow readNext() {
                BlockEntry entry = blockIterator.next();
                resultRow.setRows(
                        new InternalRow[] {
                            keySerializer.deserialize(entry.getKey()),
                            valueSerializer.deserialize(entry.getValue())
                        });
                return resultRow;
            }

            @Override
            public void releaseBatch() {
                // currently nothing to do
            }
        };
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        fileReader.close();
    }

    private boolean selectionEnd() {
        return selection != null && nextSelection >= selection.size();
    }

    private int getNextPosition() {
        return selection == null ? returnedPosition + 1 : selection.get(nextSelection);
    }
}
