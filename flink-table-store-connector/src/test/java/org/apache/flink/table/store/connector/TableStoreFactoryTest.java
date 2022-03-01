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

package org.apache.flink.table.store.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ManagedTableFactory;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.CHANGE_TRACKING;
import static org.apache.flink.table.store.file.FileStoreOptions.BUCKET;
import static org.apache.flink.table.store.file.FileStoreOptions.FILE_PATH;
import static org.apache.flink.table.store.file.FileStoreOptions.TABLE_STORE_PREFIX;
import static org.apache.flink.table.store.kafka.KafkaLogOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.table.store.log.LogOptions.CONSISTENCY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test cases for {@link TableStoreFactory}. */
public class TableStoreFactoryTest {

    private static final ObjectIdentifier TABLE_IDENTIFIER =
            ObjectIdentifier.of("catalog", "database", "table");

    private final ManagedTableFactory managedTableFactory = new TableStoreFactory();

    @TempDir private static java.nio.file.Path sharedTempDir;
    private DynamicTableFactory.Context context;

    @ParameterizedTest
    @MethodSource("providingOptions")
    public void testEnrichOptions(
            Map<String, String> sessionOptions,
            Map<String, String> tableOptions,
            Map<String, String> expectedEnrichedOptions) {
        context = createTableContext(sessionOptions, tableOptions);
        Map<String, String> actualEnrichedOptions = managedTableFactory.enrichOptions(context);
        assertThat(actualEnrichedOptions)
                .containsExactlyInAnyOrderEntriesOf(expectedEnrichedOptions);
    }

    @ParameterizedTest
    @MethodSource("providingEnrichedOptionsForCreation")
    public void testOnCreateTable(Map<String, String> enrichedOptions, boolean ignoreIfExists) {
        context = createTableContext(Collections.emptyMap(), enrichedOptions);
        Path expectedPath =
                Paths.get(
                        sharedTempDir.toAbsolutePath().toString(),
                        TABLE_IDENTIFIER.asSummaryString());
        boolean exist = expectedPath.toFile().exists();
        if (ignoreIfExists || !exist) {
            managedTableFactory.onCreateTable(context, ignoreIfExists);
            assertThat(expectedPath).exists();
        } else {
            assertThatThrownBy(() -> managedTableFactory.onCreateTable(context, false))
                    .isInstanceOf(TableException.class)
                    .hasMessageContaining(
                            String.format(
                                    "Failed to create file store path. "
                                            + "Reason: directory %s exists for table %s. "
                                            + "Suggestion: please try `DESCRIBE TABLE %s` to "
                                            + "first check whether table exists in current catalog. "
                                            + "If table exists in catalog, and data files under current path "
                                            + "are valid, please use `CREATE TABLE IF NOT EXISTS` ddl instead. "
                                            + "Otherwise, please choose another table name "
                                            + "or manually delete the current path and try again.",
                                    expectedPath,
                                    TABLE_IDENTIFIER.asSerializableString(),
                                    TABLE_IDENTIFIER.asSerializableString()));
        }
    }

    @ParameterizedTest
    @MethodSource("providingEnrichedOptionsForDrop")
    public void testOnDropTable(Map<String, String> enrichedOptions, boolean ignoreIfNotExists) {
        context = createTableContext(Collections.emptyMap(), enrichedOptions);
        Path expectedPath =
                Paths.get(
                        sharedTempDir.toAbsolutePath().toString(),
                        TABLE_IDENTIFIER.asSummaryString());
        boolean exist = expectedPath.toFile().exists();
        if (exist || ignoreIfNotExists) {
            managedTableFactory.onDropTable(context, ignoreIfNotExists);
            assertThat(expectedPath).doesNotExist();
        } else {
            assertThatThrownBy(() -> managedTableFactory.onDropTable(context, false))
                    .isInstanceOf(TableException.class)
                    .hasMessageContaining(
                            String.format(
                                    "Failed to delete file store path. "
                                            + "Reason: directory %s doesn't exist for table %s. "
                                            + "Suggestion: please try `DROP TABLE IF EXISTS` ddl instead.",
                                    expectedPath, TABLE_IDENTIFIER.asSerializableString()));
        }
    }

    // ~ Tools ------------------------------------------------------------------

    private static Stream<Arguments> providingOptions() {
        Map<String, String> enrichedOptions =
                of(
                        BUCKET.key(),
                        BUCKET.defaultValue().toString(),
                        FILE_PATH.key(),
                        sharedTempDir.toString(),
                        BOOTSTRAP_SERVERS.key(),
                        "localhost:9092",
                        CONSISTENCY.key(),
                        CONSISTENCY.defaultValue().name());

        // default
        Arguments arg0 =
                Arguments.of(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

        // set configuration under session level
        Arguments arg1 =
                Arguments.of(
                        addPrefix(enrichedOptions, (key) -> true),
                        Collections.emptyMap(),
                        enrichedOptions);

        // set configuration under table level
        Arguments arg2 = Arguments.of(Collections.emptyMap(), enrichedOptions, enrichedOptions);

        // set both session and table level configuration to test options combination
        Map<String, String> tableOptions = new HashMap<>(enrichedOptions);
        tableOptions.remove(FILE_PATH.key());
        tableOptions.remove(CONSISTENCY.key());
        Arguments arg3 =
                Arguments.of(
                        addPrefix(enrichedOptions, (key) -> !tableOptions.containsKey(key)),
                        tableOptions,
                        enrichedOptions);

        // set same key with different value to test table configuration take precedence
        Map<String, String> sessionOptions = new HashMap<>();
        sessionOptions.put(
                TABLE_STORE_PREFIX + BUCKET.key(), String.valueOf(BUCKET.defaultValue() + 1));
        Arguments arg4 = Arguments.of(sessionOptions, enrichedOptions, enrichedOptions);
        return Stream.of(arg0, arg1, arg2, arg3, arg4);
    }

    private static Stream<Arguments> providingEnrichedOptionsForCreation() {
        Map<String, String> enrichedOptions = new HashMap<>();
        enrichedOptions.put(FILE_PATH.key(), sharedTempDir.toAbsolutePath().toString());
        enrichedOptions.put(CHANGE_TRACKING.key(), String.valueOf(false));
        return Stream.of(
                Arguments.of(enrichedOptions, false),
                Arguments.of(enrichedOptions, true),
                Arguments.of(enrichedOptions, false));
    }

    private static Stream<Arguments> providingEnrichedOptionsForDrop() {
        File tablePath =
                Paths.get(
                                sharedTempDir.toAbsolutePath().toString(),
                                TABLE_IDENTIFIER.asSummaryString())
                        .toFile();
        if (!tablePath.exists()) {
            tablePath.mkdirs();
        }
        Map<String, String> enrichedOptions = new HashMap<>();
        enrichedOptions.put(FILE_PATH.key(), sharedTempDir.toAbsolutePath().toString());
        enrichedOptions.put(CHANGE_TRACKING.key(), String.valueOf(false));
        return Stream.of(
                Arguments.of(enrichedOptions, false),
                Arguments.of(enrichedOptions, true),
                Arguments.of(enrichedOptions, false));
    }

    private static Map<String, String> addPrefix(
            Map<String, String> options, Predicate<String> predicate) {
        Map<String, String> newOptions = new HashMap<>();
        options.forEach(
                (k, v) -> {
                    if (predicate.test(k)) {
                        newOptions.put(TABLE_STORE_PREFIX + k, v);
                    }
                });
        return newOptions;
    }

    private static DynamicTableFactory.Context createTableContext(
            Map<String, String> sessionOptions, Map<String, String> tableOptions) {
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.derived(),
                                "a comment",
                                Collections.emptyList(),
                                tableOptions),
                        ResolvedSchema.of(Collections.emptyList()));
        return new FactoryUtil.DefaultDynamicTableContext(
                TABLE_IDENTIFIER,
                resolvedCatalogTable,
                Collections.emptyMap(),
                Configuration.fromMap(sessionOptions),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    private static Map<String, String> of(String... kvs) {
        assert kvs != null && kvs.length % 2 == 0;
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kvs.length - 1; i += 2) {
            map.put(kvs[i], kvs[i + 1]);
        }
        return map;
    }
}
