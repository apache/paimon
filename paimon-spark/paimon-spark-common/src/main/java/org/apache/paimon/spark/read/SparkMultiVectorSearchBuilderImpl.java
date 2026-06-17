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

import org.apache.paimon.predicate.MultiVectorSearchRoute;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.source.MultiVectorSearchBuilderImpl;
import org.apache.paimon.table.source.VectorSearchBuilder;

/** Spark-aware {@link MultiVectorSearchBuilderImpl}. */
public class SparkMultiVectorSearchBuilderImpl extends MultiVectorSearchBuilderImpl {

    private static final long serialVersionUID = 1L;

    public SparkMultiVectorSearchBuilderImpl(InnerTable table) {
        super(table);
    }

    @Override
    protected VectorSearchBuilder newVectorSearchBuilder(MultiVectorSearchRoute route) {
        VectorSearchBuilder vectorSearchBuilder =
                new SparkVectorSearchBuilderImpl(table)
                        .withVector(route.vector())
                        .withVectorColumn(route.fieldName())
                        .withLimit(route.limit())
                        .withOptions(route.options());
        if (partitionFilter != null) {
            vectorSearchBuilder.withPartitionFilter(partitionFilter);
        }
        if (filter != null) {
            vectorSearchBuilder.withFilter(filter);
        }
        return vectorSearchBuilder;
    }
}
