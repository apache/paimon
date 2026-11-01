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

package org.apache.paimon.jdbc;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Serializable;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simulates Spark executors working on tables of a {@link JdbcCatalog} and counts the physical JDBC
 * connections they open.
 *
 * <p>The driver creates the table and commits data, then table objects are shipped to the
 * "executor" through Java serialization, like a Spark task closure. Resetting {@link
 * CachedJdbcClientPool} beforehand stands in for a fresh executor JVM, so every connection opened
 * afterwards is attributed to executor-side work.
 */
public class JdbcCatalogExecutorConnectionsTest {

    private static final String DATABASE = "db";

    @TempDir Path tempDir;

    private Catalog driverCatalog;

    @BeforeAll
    static void registerDriver() throws SQLException {
        DriverManager.registerDriver(new CountingDriver());
    }

    @BeforeEach
    void setUp() throws Exception {
        Options options = new Options();
        options.set(CatalogOptions.METASTORE, "jdbc");
        options.set(CatalogOptions.URI, "jdbc:counting:sqlite:" + tempDir.resolve("catalog.db"));
        options.set(CatalogOptions.WAREHOUSE, tempDir.resolve("warehouse").toUri().toString());
        options.set(JdbcCatalogOptions.CATALOG_KEY, "executor-test");
        options.set(JdbcCatalogOptions.LOCK_ENABLED, false);
        driverCatalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        driverCatalog.createDatabase(DATABASE, false);
    }

    @AfterEach
    void tearDown() throws Exception {
        driverCatalog.close();
        CachedJdbcClientPool.resetCache();
    }

    @Test
    public void testFixedBucketWriteOnExecutor() throws Exception {
        FileStoreTable table = createFixedBucketTable();
        commitOnDriver(table, row(1, 1, "a"), row(1, 2, "b"));

        BatchWriteBuilder writeBuilder = shipToExecutor(table.newBatchWriteBuilder());
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            // same partition and buckets as the driver commit, so the writer restores files
            write.write(row(1, 1, "c"));
            write.write(row(1, 2, "d"));
            write.write(row(2, 3, "e"));
            write.prepareCommit();
        }

        assertNoExecutorConnections();
    }

    @Test
    public void testFixedBucketWriteWithFullCompactionOnExecutor() throws Exception {
        FileStoreTable table = createFixedBucketTable();
        commitOnDriver(table, row(1, 1, "a"));
        commitOnDriver(table, row(1, 1, "b"));

        BatchWriteBuilder writeBuilder = shipToExecutor(table.newBatchWriteBuilder());
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            write.write(row(1, 1, "c"));
            write.compact(BinaryRow.singleColumn(1), bucketOf(table, 1, 1), true);
            write.prepareCommit();
        }

        assertNoExecutorConnections();
    }

    @Test
    public void testReadSplitsOnExecutor() throws Exception {
        FileStoreTable table = createFixedBucketTable();
        commitOnDriver(table, row(1, 1, "a"), row(2, 2, "b"));

        ReadBuilder driverReadBuilder = table.newReadBuilder();
        List<Split> driverSplits = driverReadBuilder.newScan().plan().splits();

        ReadBuilder readBuilder = shipToExecutor(driverReadBuilder);
        List<Split> splits = new ArrayList<>();
        for (Split split : driverSplits) {
            splits.add(InstantiationUtil.clone(split));
        }
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(splits)) {
            reader.forEachRemaining(rows::add);
        }

        assertThat(rows).hasSize(2);
        assertNoExecutorConnections();
    }

    @Test
    public void testScanPlanOnExecutor() throws Exception {
        FileStoreTable table = createFixedBucketTable();
        commitOnDriver(table, row(1, 1, "a"));

        ReadBuilder readBuilder = shipToExecutor(table.newReadBuilder());
        assertThat(readBuilder.newScan().plan().splits()).isNotEmpty();

        assertNoExecutorConnections();
    }

    @Test
    public void testDynamicBucketAssignOnExecutor() throws Exception {
        FileStoreTable table =
                createTable(
                        "dynamic_bucket",
                        CoreOptions.BUCKET.key(),
                        "-1",
                        CoreOptions.CHANGELOG_PRODUCER.key(),
                        "input");

        FileStoreTable executorTable = shipToExecutor(table);
        HashBucketAssigner assigner =
                new HashBucketAssigner(
                        executorTable.snapshotManager(),
                        UUID.randomUUID().toString(),
                        executorTable.store().newIndexFileHandler(),
                        1,
                        1,
                        0,
                        executorTable.coreOptions().dynamicBucketTargetRowNum(),
                        executorTable.coreOptions().dynamicBucketMaxBuckets());
        assigner.assign(BinaryRow.singleColumn(1), 42);

        assertNoExecutorConnections();
    }

    private FileStoreTable createFixedBucketTable() throws Exception {
        // mirrors the reported table: fixed buckets, input changelog, compaction in writers
        return createTable(
                "fixed_bucket",
                CoreOptions.BUCKET.key(),
                "100",
                CoreOptions.CHANGELOG_PRODUCER.key(),
                "input",
                CoreOptions.WRITE_ONLY.key(),
                "false",
                CoreOptions.CONSUMER_EXPIRATION_TIME.key(),
                "7d",
                CoreOptions.SNAPSHOT_TIME_RETAINED.key(),
                "7d");
    }

    private FileStoreTable createTable(String name, String... options) throws Exception {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .partitionKeys("pt")
                        .primaryKey("pt", "id");
        for (int i = 0; i < options.length; i += 2) {
            schema.option(options[i], options[i + 1]);
        }
        Identifier identifier = Identifier.create(DATABASE, name);
        driverCatalog.createTable(identifier, schema.build(), false);
        return (FileStoreTable) driverCatalog.getTable(identifier);
    }

    private void commitOnDriver(FileStoreTable table, InternalRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private static int bucketOf(FileStoreTable table, int pt, int id) throws Exception {
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite()) {
            return write.getBucket(row(pt, id, "x"));
        }
    }

    /** Serializes like a Spark task closure and starts from an empty JVM-wide pool cache. */
    private static <T extends Serializable> T shipToExecutor(T object) throws Exception {
        T copy = InstantiationUtil.clone(object);
        CachedJdbcClientPool.resetCache();
        CountingDriver.reset();
        return copy;
    }

    private static void assertNoExecutorConnections() {
        assertThat(CountingDriver.OPENED.get()).as("JDBC connections opened on executor").isZero();
        assertThat(CachedJdbcClientPool.clientPools())
                .as("JDBC client pools created on executor")
                .isEmpty();
    }

    private static GenericRow row(int pt, int id, String v) {
        return GenericRow.of(pt, id, BinaryString.fromString(v));
    }

    /** Delegates {@code jdbc:counting:<url>} to {@code jdbc:<url>} and counts connections. */
    private static class CountingDriver implements Driver {

        private static final String PREFIX = "jdbc:counting:";
        private static final AtomicInteger OPENED = new AtomicInteger();

        static void reset() {
            OPENED.set(0);
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            if (!acceptsURL(url)) {
                return null;
            }
            Connection connection =
                    DriverManager.getConnection("jdbc:" + url.substring(PREFIX.length()), info);
            OPENED.incrementAndGet();
            return connection;
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith(PREFIX);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }
}
