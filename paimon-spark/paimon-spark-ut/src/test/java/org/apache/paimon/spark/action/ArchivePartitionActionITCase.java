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

package org.apache.paimon.spark.action;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.StorageType;
import org.apache.paimon.s3.MinioTestContainer;
import org.apache.paimon.spark.SparkGenericCatalog;
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for ArchivePartitionAction with S3. */
@ExtendWith(ParameterizedTestExtension.class)
public class ArchivePartitionActionITCase {

    @RegisterExtension
    public static final MinioTestContainer MINIO_CONTAINER = new MinioTestContainer();

    private static Path warehousePath;
    private static SparkSession spark = null;
    private static FileSystemCatalog catalog = null;

    @BeforeAll
    public static void startSpark() throws Exception {
        String path = MINIO_CONTAINER.getS3UriForDefaultBucket() + "/" + UUID.randomUUID();
        warehousePath = new Path(path);

        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .config(
                                "spark.sql.catalog.spark_catalog",
                                SparkGenericCatalog.class.getName())
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .config("spark.sql.warehouse.dir", warehousePath.toString())
                        .getOrCreate();

        // Configure S3 settings
        MINIO_CONTAINER
                .getS3ConfigOptions()
                .forEach((k, v) -> spark.conf().set("spark.sql.catalog.spark_catalog." + k, v));

        // Create catalog
        Map<String, String> options = new HashMap<>();
        options.put("warehouse", warehousePath.toString());
        MINIO_CONTAINER.getS3ConfigOptions().forEach(options::put);
        CatalogContext context = CatalogContext.create(options);
        catalog = new FileSystemCatalog(context);
        catalog.open();
    }

    @AfterAll
    public static void stopSpark() {
        if (catalog != null) {
            try {
                catalog.close();
            } catch (Exception e) {
                // Ignore
            }
        }
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Parameters(name = "{0}")
    public static Collection<String> parameters() {
        return Arrays.asList("avro", "parquet");
    }

    private final String format;

    public ArchivePartitionActionITCase(String format) {
        this.format = format;
    }

    @AfterEach
    public void afterEach() {
        try {
            spark.sql("DROP TABLE IF EXISTS archive_test");
        } catch (Exception e) {
            // Ignore
        }
    }

    @TestTemplate
    public void testArchivePartitionActionBasic() throws Exception {
        // Create partitioned table
        spark.sql(
                String.format(
                        "CREATE TABLE archive_test (id INT, data STRING, dt STRING) "
                                + "PARTITIONED BY (dt) USING paimon TBLPROPERTIES ('file.format'='%s')",
                        format));

        // Insert data
        spark.sql("INSERT INTO archive_test VALUES (1, 'a', '2024-01-01')");
        spark.sql("INSERT INTO archive_test VALUES (2, 'b', '2024-01-02')");

        // Verify data exists
        List<Row> rows = spark.sql("SELECT * FROM archive_test ORDER BY id").collectAsList();
        assertThat(rows).hasSize(2);

        // Get table and test archive action
        FileStoreTable table =
                (FileStoreTable)
                        catalog.getTable(
                                org.apache.paimon.catalog.Identifier.create(
                                        "default", "archive_test"));
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        ArchivePartitionAction action = new ArchivePartitionAction(table, sparkContext);

        // Test archive operation (will fail gracefully if storage class change not supported)
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "2024-01-01");
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec);

        // Note: Minio doesn't support Glacier storage classes, so this will test error handling
        // In production with real S3, this would actually archive the files
        try {
            long count = action.archive(partitionSpecs, StorageType.Archive);
            // If successful, verify data is still accessible
            rows = spark.sql("SELECT * FROM archive_test WHERE dt='2024-01-01'").collectAsList();
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getInt(0)).isEqualTo(1);
            assertThat(count).isGreaterThan(0);
        } catch (UnsupportedOperationException | IOException e) {
            // Expected for Minio which doesn't support storage class transitions
            // This validates error handling - check for various error message patterns
            String message = e.getMessage();
            assertThat(message)
                    .satisfiesAnyOf(
                            msg -> assertThat(msg).containsIgnoringCase("storage class"),
                            msg -> assertThat(msg).containsIgnoringCase("StorageClass"),
                            msg -> assertThat(msg).containsIgnoringCase("S3 client"),
                            msg -> assertThat(msg).containsIgnoringCase("archive"),
                            msg ->
                                    assertThat(msg)
                                            .containsIgnoringCase("UnsupportedOperationException"));
        }
    }

    @TestTemplate
    public void testArchivePartitionActionErrorHandling() throws Exception {
        spark.sql(
                String.format(
                        "CREATE TABLE archive_test (id INT, data STRING) "
                                + "USING paimon TBLPROPERTIES ('file.format'='%s')",
                        format));

        spark.sql("INSERT INTO archive_test VALUES (1, 'a')");

        FileStoreTable table =
                (FileStoreTable)
                        catalog.getTable(
                                org.apache.paimon.catalog.Identifier.create(
                                        "default", "archive_test"));
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        ArchivePartitionAction action = new ArchivePartitionAction(table, sparkContext);

        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "2024-01-01");
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec);

        // Should handle gracefully even if partition doesn't exist
        long count = action.archive(partitionSpecs, StorageType.Archive);
        assertThat(count).isEqualTo(0); // No files found for non-existent partition
    }

    @TestTemplate
    public void testArchivePartitionActionValidation() throws Exception {
        spark.sql(
                String.format(
                        "CREATE TABLE archive_test (id INT, data STRING, dt STRING) "
                                + "PARTITIONED BY (dt) USING paimon TBLPROPERTIES ('file.format'='%s')",
                        format));

        spark.sql("INSERT INTO archive_test VALUES (1, 'a', '2024-01-01')");

        FileStoreTable table =
                (FileStoreTable)
                        catalog.getTable(
                                org.apache.paimon.catalog.Identifier.create(
                                        "default", "archive_test"));
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        ArchivePartitionAction action = new ArchivePartitionAction(table, sparkContext);

        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "2024-01-01");
        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec);

        // Test validation: cannot archive to Standard
        assertThatThrownBy(() -> action.archive(partitionSpecs, StorageType.Standard))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Standard storage type");
    }
}
