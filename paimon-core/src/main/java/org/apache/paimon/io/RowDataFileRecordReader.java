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

package org.apache.paimon.io;

import org.apache.paimon.PartitionSettedRow;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.FileHook;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** Reads {@link InternalRow} from data files. */
public class RowDataFileRecordReader implements RecordReader<InternalRow> {

    private final Path path;
    private final RecordReader<InternalRow> reader;
    @Nullable private final int[] indexMapping;
    @Nullable private final PartitionInfo partitionInfo;
    @Nullable private final CastFieldGetter[] castMapping;

    private boolean triggerOpenHooks = false;
    private final List<Consumer<String>> openFileHooks = new ArrayList<>();
    private final List<Consumer<String>> closeFileHooks = new ArrayList<>();

    public RowDataFileRecordReader(
            FileIO fileIO,
            Path path,
            long fileSize,
            FormatReaderFactory readerFactory,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable PartitionInfo partitionInfo,
            List<FileHook> hooks)
            throws IOException {
        FileUtils.checkExists(fileIO, path);
        this.path = path;
        FormatReaderContext context = new FormatReaderContext(fileIO, path, fileSize);
        this.reader = readerFactory.createReader(context);
        this.indexMapping = indexMapping;
        this.partitionInfo = partitionInfo;
        this.castMapping = castMapping;
        for (FileHook hook : hooks) {
            if (hook.getTrigger().equals(FileHook.ReaderTrigger.OPEN_FILE)) {
                openFileHooks.add(hook.getFunction());
            } else if (hook.getTrigger().equals(FileHook.ReaderTrigger.CLOSE_FILE)) {
                closeFileHooks.add(hook.getFunction());
            } else {
                throw new UnsupportedOperationException(
                        hook.getTrigger().name() + " is not supported.");
            }
        }
    }

    @Nullable
    @Override
    public RecordReader.RecordIterator<InternalRow> readBatch() throws IOException {
        triggerOpenFileHooks();

        RecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }

        if (iterator instanceof ColumnarRowIterator) {
            iterator = ((ColumnarRowIterator) iterator).mapping(partitionInfo, indexMapping);
        } else {
            if (partitionInfo != null) {
                final PartitionSettedRow partitionSettedRow =
                        PartitionSettedRow.from(partitionInfo);
                iterator = iterator.transform(partitionSettedRow::replaceRow);
            }
            if (indexMapping != null) {
                final ProjectedRow projectedRow = ProjectedRow.from(indexMapping);
                iterator = iterator.transform(projectedRow::replaceRow);
            }
        }

        if (castMapping != null) {
            final CastedRow castedRow = CastedRow.from(castMapping);
            iterator = iterator.transform(castedRow::replaceRow);
        }

        return iterator;
    }

    @Override
    public void close() throws IOException {
        reader.close();
        triggerCloseFileHooks();
    }

    private void triggerOpenFileHooks() {
        if (!triggerOpenHooks && !openFileHooks.isEmpty()) {
            for (Consumer<String> func : openFileHooks) {
                func.accept(path.toUri().toString());
            }
            triggerOpenHooks = true;
        }
    }

    private void triggerCloseFileHooks() {
        if (!closeFileHooks.isEmpty()) {
            for (Consumer<String> func : closeFileHooks) {
                func.accept(path.toUri().toString());
            }
        }
    }
}
