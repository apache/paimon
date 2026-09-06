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

package org.apache.paimon.lookup.local;

import org.apache.paimon.lookup.ValueBulkLoader;
import org.apache.paimon.lookup.sort.db.LocalKvDb;

import java.io.IOException;
import java.util.function.Consumer;

/** State bulk loader backed by a {@link LocalKvDb.BulkLoadWriter}. */
final class LocalKvBulkLoader implements ValueBulkLoader {

    private final LocalKvDb.BulkLoadWriter writer;
    private final LocalKvValueCodec valueCodec;
    private final Consumer<byte[]> cacheInvalidator;

    LocalKvBulkLoader(
            LocalKvDb db, LocalKvValueCodec valueCodec, Consumer<byte[]> cacheInvalidator) {
        try {
            this.writer = db.createBulkLoadWriter();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create LocalKvDb bulk-load writer.", e);
        }
        this.valueCodec = valueCodec;
        this.cacheInvalidator = cacheInvalidator;
    }

    @Override
    public void write(byte[] key, byte[] value) throws WriteException {
        try {
            writer.put(key, valueCodec.encode(value));
            cacheInvalidator.accept(key);
        } catch (IOException | RuntimeException e) {
            throw new WriteException(e);
        }
    }

    @Override
    public void finish() {
        try {
            writer.finish();
        } catch (IOException e) {
            throw new RuntimeException("Failed to finish LocalKvDb bulk load.", e);
        }
    }
}
