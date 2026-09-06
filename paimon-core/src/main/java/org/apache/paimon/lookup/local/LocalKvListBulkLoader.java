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

import org.apache.paimon.lookup.ListBulkLoader;
import org.apache.paimon.lookup.sort.db.LocalKvDb;
import org.apache.paimon.utils.SortUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/** List-state bulk loader which stores one packed initial list per logical key. */
final class LocalKvListBulkLoader implements ListBulkLoader {

    private final LocalKvDb.BulkLoadWriter writer;
    private final LocalKvValueCodec valueCodec;
    private final LocalKvListValueCodec listValueCodec;
    private final Function<byte[], byte[]> compositeKeyFactory;
    private final Consumer<byte[]> cacheInvalidator;

    private byte[] previousKey;

    LocalKvListBulkLoader(
            LocalKvDb db,
            LocalKvValueCodec valueCodec,
            LocalKvListValueCodec listValueCodec,
            Function<byte[], byte[]> compositeKeyFactory,
            Consumer<byte[]> cacheInvalidator) {
        try {
            this.writer = db.createBulkLoadWriter();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create LocalKvDb bulk-load writer.", e);
        }
        this.valueCodec = valueCodec;
        this.listValueCodec = listValueCodec;
        this.compositeKeyFactory = compositeKeyFactory;
        this.cacheInvalidator = cacheInvalidator;
    }

    @Override
    public void write(byte[] key, List<byte[]> values) throws WriteException {
        try {
            if (previousKey != null && SortUtil.compareBinary(previousKey, key) >= 0) {
                throw new IllegalArgumentException(
                        "Bulk-load keys must be sorted in strictly increasing order.");
            }
            previousKey = Arrays.copyOf(key, key.length);

            byte[] prefix = LocalKvCompositeKey.prefix(key);
            writer.put(
                    compositeKeyFactory.apply(prefix),
                    valueCodec.encode(listValueCodec.encodeList(values)));
            cacheInvalidator.accept(key);
        } catch (IOException | RuntimeException e) {
            writer.close();
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
