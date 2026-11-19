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

package org.apache.paimon.mergetree.dataevolution;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.reader.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads base files of a primary-key table with data evolution, replacing the values of the columns
 * held by their column files. Records are stitched before they reach the merge, so the merge sees
 * full rows.
 *
 * <p>Base files with column files are read by {@code alignedBaseFactory}, which must not push down
 * filters. Other files are read by {@code plainFactory}.
 */
public class AlignedKeyValueFileReaderFactory implements FileReaderFactory<KeyValue> {

    private final FileReaderFactory<KeyValue> plainFactory;
    private final FileReaderFactory<KeyValue> alignedBaseFactory;
    private final ColumnFileReaderFactory columnFactory;
    private final ColumnFileGroups groups;

    public AlignedKeyValueFileReaderFactory(
            FileReaderFactory<KeyValue> plainFactory,
            FileReaderFactory<KeyValue> alignedBaseFactory,
            ColumnFileReaderFactory columnFactory,
            ColumnFileGroups groups) {
        this.plainFactory = plainFactory;
        this.alignedBaseFactory = alignedBaseFactory;
        this.columnFactory = columnFactory;
        this.groups = groups;
    }

    @Override
    public RecordReader<KeyValue> createRecordReader(DataFileMeta file) throws IOException {
        List<DataFileMeta> columnFiles = groups.columnFilesOf(file);
        if (columnFiles.isEmpty()) {
            return plainFactory.createRecordReader(file);
        }

        int fieldCount = columnFactory.readValueType().getFieldCount();
        int[] rowOffsets = new int[fieldCount];
        int[] fieldOffsets = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldOffsets[i] = i;
        }
        List<RecordReader<InternalRow>> columnReaders = new ArrayList<>(columnFiles.size());
        RecordReader<KeyValue> baseReader = null;
        try {
            for (int i = 0; i < columnFiles.size(); i++) {
                DataFileMeta columnFile = columnFiles.get(i);
                boolean[] covered = columnFactory.coveredFields(columnFile);
                for (int pos = 0; pos < fieldCount; pos++) {
                    if (covered[pos]) {
                        rowOffsets[pos] = i + 1;
                    }
                }
                columnReaders.add(columnFactory.createRecordReader(columnFile));
            }
            baseReader = alignedBaseFactory.createRecordReader(file);
        } catch (IOException | RuntimeException e) {
            for (RecordReader<InternalRow> reader : columnReaders) {
                closeQuietly(reader, e);
            }
            throw e;
        }
        return new AlignedKeyValueRecordReader(
                file,
                baseReader,
                columnFiles,
                columnReaders,
                columnFactory.readValueType(),
                rowOffsets,
                fieldOffsets);
    }

    private static void closeQuietly(RecordReader<?> reader, Exception exception) {
        try {
            reader.close();
        } catch (IOException e) {
            exception.addSuppressed(e);
        }
    }
}
