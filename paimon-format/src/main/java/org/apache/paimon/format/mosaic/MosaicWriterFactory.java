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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;

import java.io.IOException;

/** Factory for creating {@link MosaicWriter} instances. */
public class MosaicWriterFactory implements FormatWriterFactory {

    private final RowType rowType;
    private final int numBuckets;
    private final int zstdLevel;
    private final long rowGroupMaxSize;

    public MosaicWriterFactory(
            RowType rowType, int numBuckets, int zstdLevel, long rowGroupMaxSize) {
        this.rowType = rowType;
        this.numBuckets = numBuckets;
        this.zstdLevel = zstdLevel;
        this.rowGroupMaxSize = rowGroupMaxSize;
    }

    @Override
    public FormatWriter create(PositionOutputStream out, String compression) throws IOException {
        return new MosaicWriter(out, rowType, numBuckets, zstdLevel, compression, rowGroupMaxSize);
    }
}
