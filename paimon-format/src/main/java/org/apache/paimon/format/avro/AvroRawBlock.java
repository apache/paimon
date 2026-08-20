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

package org.apache.paimon.format.avro;

import org.apache.avro.file.RawBlock;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Reusable compressed block from an Avro object container file. */
public final class AvroRawBlock {

    private RawBlock block;

    AvroRawBlock(RawBlock block) {
        this.block = block;
    }

    AvroRawBlock replace(RawBlock block) {
        this.block = block;
        return this;
    }

    RawBlock rawBlock() {
        return block;
    }

    public long recordCount() {
        return block.recordCount();
    }

    /** Returns an independently owned copy which is not reused by the reader. */
    public AvroRawBlock stableCopy() {
        return new AvroRawBlock(block.stableCopy());
    }

    /**
     * Lazily decompresses this block, reusing the supplied heap buffer when possible.
     *
     * <p>The returned view remains owned by this block and is invalidated when this holder is
     * reused for another block.
     */
    public ByteBuffer decompress(@Nullable ByteBuffer reuse) throws IOException {
        return block.decompress(reuse);
    }
}
