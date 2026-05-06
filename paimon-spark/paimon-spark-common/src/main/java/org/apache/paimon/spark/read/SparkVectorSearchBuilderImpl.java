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

import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.source.VectorRead;
import org.apache.paimon.table.source.VectorSearchBuilderImpl;

/**
 * Spark-aware {@link VectorSearchBuilderImpl} which produces a {@link SparkVectorReadImpl} so the
 * per-split vector index evaluation is dispatched through Spark instead of the local thread pool.
 */
public class SparkVectorSearchBuilderImpl extends VectorSearchBuilderImpl {

    private static final long serialVersionUID = 1L;

    public SparkVectorSearchBuilderImpl(InnerTable table) {
        super(table);
    }

    @Override
    public VectorRead newVectorRead() {
        return new SparkVectorReadImpl(table, filter, limit, vectorColumn, vector);
    }
}
