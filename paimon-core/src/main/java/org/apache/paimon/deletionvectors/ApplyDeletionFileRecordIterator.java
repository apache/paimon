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

import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link FileRecordIterator} wraps a {@link FileRecordIterator} and {@link DeletionVector}. */
public class ApplyDeletionFileRecordIterator<T> implements FileRecordIterator<T> {

    private final FileRecordIterator<T> iterator;
    private final DeletionVector deletionVector;

    public ApplyDeletionFileRecordIterator(
            FileRecordIterator<T> iterator, DeletionVector deletionVector) {
        this.iterator = iterator;
        this.deletionVector = deletionVector;
    }

    public FileRecordIterator<T> iterator() {
        return iterator;
    }

    public DeletionVector deletionVector() {
        return deletionVector;
    }

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
    public T next() throws IOException {
        while (true) {
            T next = iterator.next();
            if (next == null) {
                return null;
            }
            if (!deletionVector.isDeleted(returnedPosition())) {
                return next;
            }
        }
    }

    @Override
    public void releaseBatch() {
        iterator.releaseBatch();
    }
}
