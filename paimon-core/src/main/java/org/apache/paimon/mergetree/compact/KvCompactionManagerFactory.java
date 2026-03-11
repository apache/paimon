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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/** Factory to create a {@link CompactManager} for primary key table. */
public interface KvCompactionManagerFactory extends Closeable {

    static KvCompactionManagerFactory create(
            KeyValueFileReaderFactory.Builder readerFactoryBuilder,
            KeyValueFileWriterFactory.Builder writerFactoryBuilder,
            Supplier<Comparator<InternalRow>> keyComparatorSupplier,
            Supplier<FieldsComparator> udsComparatorSupplier,
            Supplier<RecordEqualiser> logDedupEqualSupplier,
            MergeFunctionFactory<KeyValue> mfFactory,
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            RowType partitionType,
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            @Nullable RecordLevelExpire recordLevelExpire,
            CacheManager cacheManager) {
        return new MergeTreeCompactManagerFactory(
                readerFactoryBuilder,
                writerFactoryBuilder,
                keyComparatorSupplier,
                udsComparatorSupplier,
                logDedupEqualSupplier,
                mfFactory,
                options,
                keyType,
                valueType,
                partitionType,
                fileIO,
                schemaManager,
                schema,
                recordLevelExpire,
                cacheManager);
    }

    void withIOManager(@Nullable IOManager ioManager);

    void withCompactionMetrics(
            @Nullable CompactionMetrics compactionMetrics);

    /** Create a {@link CompactManager} for the given partition and bucket. */
    CompactManager create(
            BinaryRow partition,
            int bucket,
            ExecutorService compactExecutor,
            List<DataFileMeta> restoreFiles,
            @Nullable BucketedDvMaintainer dvMaintainer);
}
