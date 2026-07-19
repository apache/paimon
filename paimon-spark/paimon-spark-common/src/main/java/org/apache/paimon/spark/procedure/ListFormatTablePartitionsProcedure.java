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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.format.FormatTablePartitionCatalog;
import org.apache.paimon.spark.format.FormatTablePartitionPage;
import org.apache.paimon.spark.format.PaimonFormatTable;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Preconditions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Lists managed format-table partitions with bounded pagination. */
public class ListFormatTablePartitionsProcedure extends BaseProcedure {

    private static final int DEFAULT_LIMIT = 1000;
    private static final int MAX_LIMIT = 10000;
    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("where", StringType),
                ProcedureParameter.optional("limit", IntegerType),
                ProcedureParameter.optional("page_token", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("partition", StringType, false, Metadata.empty()),
                        new StructField("next_page_token", StringType, true, Metadata.empty())
                    });

    protected ListFormatTablePartitionsProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier ident = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String where = args.isNullAt(1) ? null : args.getString(1);
        int limit = args.isNullAt(2) ? DEFAULT_LIMIT : args.getInt(2);
        String pageToken = args.isNullAt(3) ? null : args.getString(3);
        Preconditions.checkArgument(
                limit > 0 && limit <= MAX_LIMIT,
                "limit must be between 1 and %s, but was %s",
                MAX_LIMIT,
                limit);

        PaimonFormatTable sparkTable = loadFormatTable(ident);
        FormatTable formatTable = sparkTable.table();
        FormatTablePartitionCatalog partitionCatalog = managedPartitionCatalog(sparkTable);

        RowType partitionType = formatTable.partitionType();
        PartitionPredicate partitionPredicate =
                SparkProcedureUtils.convertToPartitionPredicate(
                        where, partitionType, spark(), createRelation(ident, sparkTable));
        Predicate<Map<String, String>> predicate = null;
        if (partitionPredicate != null) {
            InternalRowSerializer serializer = new InternalRowSerializer(partitionType);
            predicate =
                    spec -> {
                        GenericRow row =
                                InternalRowPartitionComputer.convertSpecToInternalRow(
                                        spec, partitionType, formatTable.defaultPartName());
                        BinaryRow binaryRow = serializer.toBinaryRow(row);
                        return partitionPredicate.test(binaryRow);
                    };
        }

        FormatTablePartitionPage page =
                listPage(partitionCatalog, Collections.emptyMap(), pageToken, limit, predicate);
        UTF8String nextToken =
                page.nextPageToken() == null ? null : UTF8String.fromString(page.nextPageToken());
        return page.partitions().stream()
                .map(
                        spec ->
                                newInternalRow(
                                        UTF8String.fromString(partitionPath(spec, partitionType)),
                                        nextToken))
                .toArray(InternalRow[]::new);
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ListFormatTablePartitionsProcedure>() {
            @Override
            protected ListFormatTablePartitionsProcedure doBuild() {
                return new ListFormatTablePartitionsProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ListFormatTablePartitionsProcedure";
    }

    /**
     * Fetch one raw catalog page of {@code pageSize} partitions and, when a filter is present,
     * return only the matching rows. Filtered pages may therefore be sparse (fewer than {@code
     * pageSize} rows); the catalog's own next-page token is passed through verbatim. Raw pages
     * whose rows are all filtered out are skipped within the same call, so an empty result always
     * means the listing is exhausted.
     */
    static FormatTablePartitionPage listPage(
            FormatTablePartitionCatalog catalog,
            Map<String, String> prefix,
            String pageToken,
            int pageSize,
            Predicate<Map<String, String>> predicate) {
        String token = normalizePageToken(pageToken);
        while (true) {
            FormatTablePartitionPage page = catalog.listPartitions(prefix, token, pageSize);
            List<Map<String, String>> partitions = page.partitions();
            // An overshooting page would silently violate the caller's limit and duplicate rows
            // on the next page, so it is the one gateway contract worth asserting here.
            Preconditions.checkState(
                    partitions.size() <= pageSize,
                    "Format table partition catalog returned %s partitions for a page of %s.",
                    partitions.size(),
                    pageSize);
            String nextToken = normalizePageToken(page.nextPageToken());
            if (predicate != null) {
                List<Map<String, String>> filtered = new ArrayList<>();
                for (Map<String, String> spec : partitions) {
                    if (predicate.test(spec)) {
                        filtered.add(spec);
                    }
                }
                // The next-page token travels on result rows, so a page whose rows are all
                // filtered out would lose the cursor. Skip ahead until at least one row matches
                // or the listing is exhausted; an empty result therefore always means "done".
                if (filtered.isEmpty() && nextToken != null) {
                    token = nextToken;
                    continue;
                }
                partitions = filtered;
            }
            return new FormatTablePartitionPage(partitions, nextToken);
        }
    }

    private static String normalizePageToken(String pageToken) {
        return pageToken == null || pageToken.isEmpty() ? null : pageToken;
    }
}
