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

package org.apache.paimon.spark.read;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.source.BatchVectorRead;
import org.apache.paimon.table.source.BatchVectorSearchBuilderImpl;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Spark-aware {@link BatchVectorSearchBuilderImpl}. Index splits and primary-key buckets are
 * evaluated across the Spark cluster, one result per query vector in input order.
 *
 * <p>Call this from the driver, where an active Spark session exists. When distribution is enabled,
 * LATERAL batch search collects query vectors on the driver and calls this there. Executor tasks
 * cannot submit another Spark job.
 */
public class SparkBatchVectorSearchBuilderImpl extends BatchVectorSearchBuilderImpl {

    private static final long serialVersionUID = 1L;

    public SparkBatchVectorSearchBuilderImpl(InnerTable table) {
        super(table);
    }

    @Override
    public BatchVectorRead newBatchVectorRead() {
        TableQueryAuthResult.rejectSearchUnderQueryAuth(table);
        checkArgument(limit > 0, "Limit must be positive, set via withLimit()");
        checkNotNull(vectorColumn, "Vector column must be set via withVectorColumn()");
        checkArgument(
                vectors != null && vectors.length > 0, "vectors must be set via withVectors()");
        for (float[] vector : vectors) {
            checkNotNull(vector, "Search vector element cannot be null");
        }
        if (isPrimaryKeyVectorSearch()) {
            return new SparkPrimaryKeyBatchVectorRead(
                    table, vectorColumn, vectors, limit, options, filter);
        }
        return new SparkDataEvolutionBatchVectorRead(
                table, partitionFilter, filter, limit, vectorColumn, vectors, options);
    }
}
