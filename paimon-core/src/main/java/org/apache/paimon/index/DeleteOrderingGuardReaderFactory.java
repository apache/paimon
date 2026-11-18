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

package org.apache.paimon.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.mergetree.compact.DeleteOrderingGuardReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/** Opens immutable segments using their source schema and projects metadata by stable field id. */
public class DeleteOrderingGuardReaderFactory {

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final CoreOptions options;
    private final RowType keyType;
    private final RowType valueType;
    private final FileStorePathFactory pathFactory;
    private final CacheManager cacheManager;

    public DeleteOrderingGuardReaderFactory(
            FileIO fileIO,
            SchemaManager schemaManager,
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            FileStorePathFactory pathFactory,
            CacheManager cacheManager) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.options = options;
        this.keyType = keyType;
        this.valueType = valueType;
        this.pathFactory = pathFactory;
        this.cacheManager = cacheManager;
    }

    public DeleteOrderingGuardReader open(
            BinaryRow partition,
            int bucket,
            List<IndexFileMeta> files,
            RowType readType,
            Comparator<InternalRow> keyComparator)
            throws IOException {
        List<RecordReader<KeyValue>> readers = new ArrayList<>();
        IndexPathFactory paths = pathFactory.indexFileFactory(partition, bucket);
        try {
            for (IndexFileMeta file : files) {
                long schemaId = DeleteOrderingGuardFile.readSchemaId(fileIO, paths.toPath(file));
                TableSchema source = schemaManager.schema(schemaId);
                CoreOptions sourceOptions = CoreOptions.fromMap(source.options());
                if (!DeleteOrderingGuardPolicy.orderingOptions(sourceOptions)
                        .equals(DeleteOrderingGuardPolicy.orderingOptions(options))) {
                    throw new IOException(
                            "Delete ordering guard ordering/deletion policy changed since schema "
                                    + schemaId
                                    + "; an ordering migration is required.");
                }
                RowType sourceKeyType =
                        new RowType(
                                PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR.keyFields(
                                        source));
                if (!sourceKeyType.equals(keyType)) {
                    throw new IOException("Delete ordering guard primary key schema changed.");
                }
                DeleteOrderingGuardSerializer serializer =
                        new DeleteOrderingGuardSerializer(
                                schemaId, keyType, source.logicalRowType(), sourceOptions);
                DeleteOrderingGuardSerializer current =
                        new DeleteOrderingGuardSerializer(0, keyType, valueType, options);
                if (!serializer.orderingType().equals(current.orderingType())) {
                    throw new IOException("Delete ordering guard ordering field schema changed.");
                }
                Function<DeleteOrderingGuard, KeyValue> projection =
                        projection(serializer.orderingType(), readType);
                readers.add(
                        new DeleteOrderingGuardFile(fileIO, paths, serializer, cacheManager, 4096)
                                .read(file)
                                .transform(projection));
            }
        } catch (IOException | RuntimeException | Error failure) {
            for (RecordReader<KeyValue> reader : readers) {
                try {
                    reader.close();
                } catch (Throwable cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            throw failure;
        }
        return new DeleteOrderingGuardReader(readers, keyComparator);
    }

    private static Function<DeleteOrderingGuard, KeyValue> projection(
            RowType metadataType, RowType readType) throws IOException {
        int[] positions = new int[readType.getFieldCount()];
        InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[positions.length];
        for (int i = 0; i < positions.length; i++) {
            DataField field = readType.getFields().get(i);
            positions[i] = -1;
            for (int j = 0; j < metadataType.getFieldCount(); j++) {
                DataField metadata = metadataType.getFields().get(j);
                if (field.id() == metadata.id()) {
                    if (!field.type().copy(true).equals(metadata.type().copy(true))) {
                        throw new IOException("Projected delete ordering field type changed.");
                    }
                    positions[i] = j;
                    getters[i] = InternalRow.createFieldGetter(metadata.type(), j);
                    break;
                }
            }
        }
        return guard -> {
            GenericRow sparse = new GenericRow(positions.length);
            for (int i = 0; i < positions.length; i++) {
                if (positions[i] >= 0) {
                    sparse.setField(i, getters[i].getFieldOrNull(guard.ordering()));
                }
            }
            return new KeyValue()
                    .replace(guard.key(), guard.sequenceNumber(), RowKind.DELETE, sparse);
        };
    }
}
