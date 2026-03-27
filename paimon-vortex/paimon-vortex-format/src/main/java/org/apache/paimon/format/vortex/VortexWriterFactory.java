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

package org.apache.paimon.format.vortex;

import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.format.vortex.VortexUtils.toVortexSpecified;

/** A factory to create Vortex {@link FormatWriter}. */
public class VortexWriterFactory implements FormatWriterFactory, SupportsDirectWrite {

    private static final Logger LOG = LoggerFactory.getLogger(VortexWriterFactory.class);

    private final RowType rowType;
    private final Supplier<ArrowFormatWriter> arrowFormatWriterSupplier;

    public VortexWriterFactory(
            RowType rowType, Supplier<ArrowFormatWriter> arrowFormatWriterSupplier) {
        this.rowType = rowType;
        this.arrowFormatWriterSupplier = arrowFormatWriterSupplier;
    }

    @Override
    public FormatWriter create(PositionOutputStream positionOutputStream, String compression) {
        throw new UnsupportedOperationException(
                "VortexWriterFactory does not support PositionOutputStream, "
                        + "please use create(FileIO fileIO, Path path, String compression) instead.");
    }

    @Override
    public FormatWriter create(FileIO fileIO, Path path, String compression) throws IOException {
        Pair<Path, Map<String, String>> vortexSpecified = toVortexSpecified(fileIO, path);
        return new VortexRecordsWriter(
                rowType,
                arrowFormatWriterSupplier.get(),
                vortexSpecified.getLeft(),
                vortexSpecified.getRight());
    }
}
