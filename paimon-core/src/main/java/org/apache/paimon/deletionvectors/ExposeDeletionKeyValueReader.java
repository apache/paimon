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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.KeyValue;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link FileRecordReader} that is aware of a {@link DeletionVector}. For each record returned by
 * the wrapped reader, if the corresponding position is marked as deleted in the deletion vector,
 * the record's value kind is replaced with {@link RowKind#DELETE} so that downstream merge
 * functions see the deletion as a real tombstone record.
 */
public class ExposeDeletionKeyValueReader implements FileRecordReader<KeyValue> {

    private final FileRecordReader<KeyValue> reader;
    private final DeletionVectorJudger deletionVector;

    public ExposeDeletionKeyValueReader(
            FileRecordReader<KeyValue> reader, DeletionVector deletionVector) {
        this.reader = reader;
        this.deletionVector = deletionVector;
    }

    @Nullable
    @Override
    public FileRecordIterator<KeyValue> readBatch() throws IOException {
        FileRecordIterator<KeyValue> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }
        return new FileRecordIterator<KeyValue>() {
            @Override
            public long returnedPosition() {
                return iterator.returnedPosition();
            }

            @Override
            public Path filePath() {
                return iterator.filePath();
            }

            @Nullable
            @Override
            public KeyValue next() throws IOException {
                KeyValue kv = iterator.next();
                if (kv == null) {
                    return null;
                }
                if (deletionVector.isDeleted(returnedPosition())) {
                    kv.replaceValueKind(RowKind.DELETE);
                }
                return kv;
            }

            @Override
            public void releaseBatch() {
                iterator.releaseBatch();
            }
        };
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
