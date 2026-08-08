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

package org.apache.paimon.flink.query;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.query.GlobalIndexQueryLocationImpl;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.service.client.GlobalIndexQueryClient;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.QuerySpec;
import org.apache.paimon.table.query.TableQuery;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Client-side {@link TableQuery} for the dedicated append global-index query service. */
public class RemoteGlobalIndexTableQuery implements TableQuery {

    private final FileStoreTable table;
    private final QuerySpec spec;
    private final GlobalIndexQueryClient client;
    private final InternalRowSerializer keySerializer;
    private final InternalRowSerializer valueSerializer;

    public RemoteGlobalIndexTableQuery(Table table, String lookupField, List<String> valueFields) {
        this.table = (FileStoreTable) table;
        this.spec = GlobalIndexQueryServiceUtils.querySpec(this.table, lookupField, valueFields);
        ServiceManager manager = this.table.store().newServiceManager();
        this.client =
                new GlobalIndexQueryClient(
                        new GlobalIndexQueryLocationImpl(
                                manager,
                                this.table.uuid(),
                                this.table.coreOptions().branch(),
                                this.table.schema().id(),
                                spec),
                        1);
        this.keySerializer =
                InternalSerializers.create(
                        TypeUtils.project(this.table.rowType(), new int[] {spec.lookupPosition()}));
        this.valueSerializer =
                InternalSerializers.create(
                        TypeUtils.project(this.table.rowType(), spec.valuePositions()));
    }

    public static boolean isRemoteServiceAvailable(
            FileStoreTable table, String lookupField, List<String> valueFields) {
        QuerySpec spec = GlobalIndexQueryServiceUtils.querySpec(table, lookupField, valueFields);
        return new GlobalIndexQueryLocationImpl(
                        table.store().newServiceManager(),
                        table.uuid(),
                        table.coreOptions().branch(),
                        table.schema().id(),
                        spec)
                .isServiceReady();
    }

    @Nullable
    @Override
    public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException {
        BinaryRow binaryKey = keySerializer.toBinaryRow(key).copy();
        checkArgument(!binaryKey.anyNull(), "Global-index query lookup key must not be null.");
        try {
            return client.getValues(new BinaryRow[] {binaryKey}).get()[0];
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException(cause);
        }
    }

    @Override
    public RemoteGlobalIndexTableQuery withValueProjection(int[] projection) {
        checkArgument(
                Arrays.equals(spec.valuePositions(), projection),
                "Global-index query value projection is fixed by the service ID.");
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        return valueSerializer;
    }

    @Override
    public void close() {
        client.shutdown();
    }

    @VisibleForTesting
    public CompletableFuture<Void> cancel() {
        return client.shutdownFuture();
    }
}
