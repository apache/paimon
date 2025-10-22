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

package org.apache.paimon.data;

import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;
import java.util.function.Supplier;

/** Blob supplied by stream. */
public class BlobStream implements Blob {

    private final Supplier<SeekableInputStream> inputStreamSupplier;

    public BlobStream(Supplier<SeekableInputStream> inputStreamSupplier) {
        this.inputStreamSupplier = inputStreamSupplier;
    }

    @Override
    public byte[] toData() {
        throw new UnsupportedOperationException("Blob stream can not convert to data.");
    }

    @Override
    public BlobDescriptor toDescriptor() {
        throw new UnsupportedOperationException("Blob stream can not convert to descriptor.");
    }

    @Override
    public SeekableInputStream newInputStream() throws IOException {
        return inputStreamSupplier.get();
    }
}
