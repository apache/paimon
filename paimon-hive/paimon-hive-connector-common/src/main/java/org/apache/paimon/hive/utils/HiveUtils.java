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

package org.apache.paimon.hive.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.LocationKeyExtractor;
import org.apache.paimon.hive.SearchArgumentToPredicateConverter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.utils.PartitionPathUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.options.OptionsUtils.PAIMON_PREFIX;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;

/** Utils for create {@link FileStoreTable} and {@link Predicate}. */
public class HiveUtils {

    public static FileStoreTable createFileStoreTable(JobConf jobConf) {
        Options options = extractCatalogConfig(jobConf);
        options.set(CoreOptions.PATH, LocationKeyExtractor.getPaimonLocation(jobConf));
        CatalogContext catalogContext = CatalogContext.create(options);
        return FileStoreTableFactory.create(catalogContext);
    }

    public static Optional<Predicate> createPredicate(
            TableSchema tableSchema, JobConf jobConf, boolean limitToReadColumnNames) {
        SearchArgument searchArgument = ConvertAstToSearchArg.createFromConf(jobConf);
        if (searchArgument == null) {
            return Optional.empty();
        }
        Set<String> readColumnNames = null;
        if (limitToReadColumnNames) {
            readColumnNames =
                    Arrays.stream(ColumnProjectionUtils.getReadColumnNames(jobConf))
                            .collect(Collectors.toSet());
        }
        String tagToPartField =
                tableSchema.options().get(CoreOptions.METASTORE_TAG_TO_PARTITION.key());
        if (tagToPartField != null) {
            // exclude tagToPartField, this should be done in Hive partition prune
            // cannot find the field in paimon table schema
            if (readColumnNames == null) {
                readColumnNames = new HashSet<>(tableSchema.fieldNames());
            }
            readColumnNames.remove(tagToPartField);
        }
        SearchArgumentToPredicateConverter converter =
                new SearchArgumentToPredicateConverter(
                        searchArgument,
                        tableSchema.fieldNames(),
                        tableSchema.logicalRowType().getFieldTypes(),
                        readColumnNames);
        return converter.convert();
    }

    /** Extract paimon catalog conf from Hive conf. */
    public static Options extractCatalogConfig(Configuration hiveConf) {
        Map<String, String> configMap =
                hiveConf == null
                        ? new HashMap<>()
                        : convertToPropertiesPrefixKey(
                                hiveConf, PAIMON_PREFIX, v -> !"NULL".equalsIgnoreCase(v));
        return Options.fromMap(configMap);
    }

    /** Extract tag name from location, partition field should be tag name. */
    public static String extractTagName(String location, String tagToPartField) {
        LinkedHashMap<String, String> partSpec =
                PartitionPathUtils.extractPartitionSpecFromPath(new Path(location));
        return partSpec.get(tagToPartField);
    }
}
