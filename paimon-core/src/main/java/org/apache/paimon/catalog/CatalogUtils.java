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

package org.apache.paimon.catalog;

import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.apache.paimon.CoreOptions.PARTITION_GENERATE_LEGCY_NAME;
import static org.apache.paimon.catalog.Catalog.TABLE_DEFAULT_OPTION_PREFIX;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;

/** Utils for {@link Catalog}. */
public class CatalogUtils {

    public static Path path(String warehouse, String database, String table) {
        return new Path(String.format("%s/%s.db/%s", warehouse, database, table));
    }

    public static String stringifyPath(String warehouse, String database, String table) {
        return String.format("%s/%s.db/%s", warehouse, database, table);
    }

    public static String warehouse(String path) {
        return new Path(path).getParent().getParent().toString();
    }

    public static String database(Path path) {
        return SchemaManager.identifierFromPath(path.toString(), false).getDatabaseName();
    }

    public static String database(String path) {
        return SchemaManager.identifierFromPath(path, false).getDatabaseName();
    }

    public static String table(Path path) {
        return SchemaManager.identifierFromPath(path.toString(), false).getObjectName();
    }

    public static String table(String path) {
        return SchemaManager.identifierFromPath(path, false).getObjectName();
    }

    public static Map<String, String> tableDefaultOptions(Map<String, String> options) {
        return convertToPropertiesPrefixKey(options, TABLE_DEFAULT_OPTION_PREFIX);
    }

    public static List<Partition> listPartitionsFromFileSystem(Table table) {
        Options options = Options.fromMap(table.options());
        InternalRowPartitionComputer computer =
                new InternalRowPartitionComputer(
                        options.get(PARTITION_DEFAULT_NAME),
                        table.rowType(),
                        table.partitionKeys().toArray(new String[0]),
                        options.get(PARTITION_GENERATE_LEGCY_NAME));
        List<PartitionEntry> partitionEntries =
                table.newReadBuilder().newScan().listPartitionEntries();
        List<Partition> partitions = new ArrayList<>(partitionEntries.size());
        for (PartitionEntry entry : partitionEntries) {
            partitions.add(
                    new Partition(
                            computer.generatePartValues(entry.partition()),
                            entry.recordCount(),
                            entry.fileSizeInBytes(),
                            entry.fileCount(),
                            entry.lastFileCreationTime()));
        }
        return partitions;
    }
}
