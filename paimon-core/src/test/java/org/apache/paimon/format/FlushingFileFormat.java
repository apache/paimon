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

package org.apache.paimon.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** A special {@link FileFormat} which flushes for every added element. */
public class FlushingFileFormat extends FileFormat {

    private final FileFormat format;

    public FlushingFileFormat(String identifier) {
        super(identifier);
        this.format = FileFormat.fromIdentifier(identifier, new Options());
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType projectedRowType, @Nullable List<Predicate> filters) {
        return format.createReaderFactory(projectedRowType, filters);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return (PositionOutputStream, level) -> {
            FormatWriter wrapped =
                    format.createWriterFactory(type)
                            .create(
                                    PositionOutputStream,
                                    CoreOptions.FILE_COMPRESSION.defaultValue());
            InternalRowSerializer serializer = new InternalRowSerializer(type);
            return new FormatWriter() {

                long totalSize = 0;

                @Override
                public void addElement(InternalRow row) throws IOException {
                    wrapped.addElement(row);
                    totalSize += serializer.toBinaryRow(row).getSizeInBytes();
                }

                @Override
                public void close() throws IOException {
                    wrapped.close();
                }

                @Override
                public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
                    return totalSize > targetSize;
                }
            };
        };
    }

    @Override
    public void validateDataFields(RowType rowType) {}
}
