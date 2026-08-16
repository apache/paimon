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

package org.apache.paimon.format.lance;

import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.format.lance.jni.LanceWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.Pair;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.format.lance.LanceUtils.toLanceSpecifiedForWriter;

/** A factory to create Lance {@link FormatWriter}. */
public class LanceWriterFactory implements FormatWriterFactory, SupportsDirectWrite {

    private final Supplier<ArrowFormatWriter> arrowFormatWriterSupplier;

    public LanceWriterFactory(Supplier<ArrowFormatWriter> arrowFormatWriterSupplier) {
        this.arrowFormatWriterSupplier = arrowFormatWriterSupplier;
    }

    @Override
    public FormatWriter create(PositionOutputStream positionOutputStream, String compression)
            throws IOException {
        throw new UnsupportedOperationException(
                "LanceWriterFactory does not support PositionOutputStream, please use create(FileIO fileIO, String path, String compression) instead.");
    }

    @Override
    public FormatWriter create(FileIO fileIO, Path path, String compression) throws IOException {
        Pair<Path, Map<String, String>> lanceSpecified = toLanceSpecifiedForWriter(fileIO, path);
        LanceWriter lanceWriter =
                new LanceWriter(lanceSpecified.getLeft().toString(), lanceSpecified.getRight());
        return new LanceRecordsWriter(
                lanceWriter::getWrittenPosition, arrowFormatWriterSupplier.get(), lanceWriter);
    }
}
