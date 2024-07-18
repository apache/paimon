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

package org.apache.paimon.compression;

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;

/** Implementation of {@link BlockCompressionFactory} for airlift compressors. */
public class AirCompressorFactory implements BlockCompressionFactory {

    private final BlockCompressionType type;
    private final Compressor internalCompressor;
    private final Decompressor internalDecompressor;

    public AirCompressorFactory(
            BlockCompressionType type,
            Compressor internalCompressor,
            Decompressor internalDecompressor) {
        this.type = type;
        this.internalCompressor = internalCompressor;
        this.internalDecompressor = internalDecompressor;
    }

    @Override
    public BlockCompressionType getCompressionType() {
        return type;
    }

    @Override
    public BlockCompressor getCompressor() {
        return new AirBlockCompressor(internalCompressor);
    }

    @Override
    public BlockDecompressor getDecompressor() {
        return new AirBlockDecompressor(internalDecompressor);
    }
}
