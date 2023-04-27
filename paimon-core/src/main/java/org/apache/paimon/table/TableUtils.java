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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateFilter;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

/** Utils for Table. TODO we can introduce LocalAction maybe? */
public class TableUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TableUtils.class);

    /**
     * Delete according to filters.
     *
     * <p>NOTE: This method is only suitable for deletion of small amount of data.
     */
    public static void deleteWhere(Table table, List<Predicate> filters, Lock.Factory lockFactory) {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(filters);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(splits);
                BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit =
                        ((InnerTableCommit) writeBuilder.newCommit())
                                .withLock(lockFactory.create())) {
            CloseableIterator<InternalRow> iterator = reader.toCloseableIterator();
            PredicateFilter filter = new PredicateFilter(table.rowType(), filters);
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                if (filter.test(row)) {
                    row.setRowKind(RowKind.DELETE);
                    write.write(row);
                }
            }

            commit.commit(write.prepareCommit());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static FileIO tableFileIO(CatalogContext context) {
        FileIO fileIO;
        try {
            fileIO = FileIO.get(CoreOptions.path(context.options()), context);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return fileIO;
    }

    public static Optional<TableSchema> schema(CatalogContext context) {
        FileIO fileIO = tableFileIO(context);
        Path tablePath = CoreOptions.path(context.options());
        return schema(fileIO, tablePath);
    }

    public static Optional<TableSchema> schema(FileIO fileIO, Path tablePath) {
        return new SchemaManager(fileIO, tablePath).latest();
    }

    public static TableSchema createTable(CatalogContext context, Schema schema) {
        FileIO fileIO = tableFileIO(context);
        Path tablePath = CoreOptions.path(context.options());
        try {
            return new SchemaManager(fileIO, tablePath).createTable(schema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Delete the table store file through the table path parameter {@link CatalogContext}.
     *
     * @param context context of catalog
     */
    public static void dropTable(CatalogContext context) {
        FileIO fileIO = tableFileIO(context);
        Path tablePath = CoreOptions.path(context.options());
        try {
            if (fileIO.exists(tablePath)) {
                fileIO.deleteDirectoryQuietly(tablePath);
            }
        } catch (IOException e) {
            LOG.error("Delete directory[{}] fail for table.", tablePath, e);
        }
    }
}
