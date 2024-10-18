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
import org.apache.paimon.query.QueryLocationImpl;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.service.client.KvQueryClient;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.query.TableQuery;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;

/** Implementation for {@link TableQuery} to lookup data from remote service. */
public class RemoteTableQuery implements TableQuery {
    private final Object lock = new Object();
    private final FileStoreTable table;
    private final KvQueryClient client;
    private final InternalRowSerializer keySerializer;

    @Nullable private int[] projection;

    public RemoteTableQuery(Table table) {
        this.table = (FileStoreTable) table;
        ServiceManager manager = this.table.store().newServiceManager();
        this.client = new KvQueryClient(new QueryLocationImpl(manager), 1);
        this.keySerializer =
                InternalSerializers.create(TypeUtils.project(table.rowType(), table.primaryKeys()));
    }

    public static boolean isRemoteServiceAvailable(FileStoreTable table) {
        return table.store().newServiceManager().service(PRIMARY_KEY_LOOKUP).isPresent();
    }

    @Nullable
    @Override
    public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException {
        BinaryRow row;
        BinaryRow binaryRowKey;
        try {
            synchronized (lock) {
                binaryRowKey = keySerializer.toBinaryRow(key);
            }
            row = client.getValues(partition, bucket, new BinaryRow[] {binaryRowKey}).get()[0];
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }

        if (projection == null) {
            return row;
        }

        if (row == null) {
            return null;
        }

        return ProjectedRow.from(projection).replaceRow(row);
    }

    @Override
    public RemoteTableQuery withValueProjection(int[] projection) {
        this.projection = projection;
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        return InternalSerializers.create(TypeUtils.project(table.rowType(), projection));
    }

    @Override
    public void close() throws IOException {
        client.shutdown();
    }

    @VisibleForTesting
    public CompletableFuture<Void> cancel() {
        return client.shutdownFuture();
    }
}
