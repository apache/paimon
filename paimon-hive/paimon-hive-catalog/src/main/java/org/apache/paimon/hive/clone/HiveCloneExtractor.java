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

package org.apache.paimon.hive.clone;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** An interface for hive clone schema and files extractor. */
public interface HiveCloneExtractor {

    boolean matches(Table table);

    List<FieldSchema> extractSchema(
            IMetaStoreClient client, Table hiveTable, String database, String table)
            throws Exception;

    List<HivePartitionFiles> extractFiles(
            Map<String, String> catalogOptions,
            IMetaStoreClient client,
            Table table,
            FileIO fileIO,
            Identifier identifier,
            RowType partitionRowType,
            String defaultPartitionName,
            @Nullable PartitionPredicate predicate)
            throws Exception;

    List<String> extractPartitionKeys(Table table);

    Map<String, String> extractOptions(Table table);

    boolean supportCloneSplits(String format);

    List<HiveCloneExtractor> EXTRACTORS =
            FactoryUtil.discoverFactories(
                    HiveCloneExtractor.class.getClassLoader(), HiveCloneExtractor.class);

    static HiveCloneExtractor getExtractor(Table table) {
        for (HiveCloneExtractor extractor : EXTRACTORS) {
            if (extractor.matches(table)) {
                return extractor;
            }
        }
        return HiveTableCloneExtractor.INSTANCE;
    }
}
