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

package org.apache.paimon.table.source;

import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;

import java.io.Serializable;

/** Builder to build vector search. */
public interface VectorSearchBuilder extends Serializable {

    /** Push partition filters. */
    VectorSearchBuilder withPartitionFilter(PartitionPredicate partitionPredicate);

    /** Push pre-filter for vector search. */
    VectorSearchBuilder withFilter(Predicate predicate);

    /** The top k results to return. */
    VectorSearchBuilder withLimit(int limit);

    /** The vector column to search. */
    VectorSearchBuilder withVectorColumn(String name);

    /** The vector to search. */
    VectorSearchBuilder withVector(float[] vector);

    /** Create vector scan to scan index files. */
    VectorScan newVectorScan();

    /** Create vector read to read index files. */
    VectorRead newVectorRead();

    /** Execute vector index search in local. */
    default GlobalIndexResult executeLocal() {
        return newVectorRead().read(newVectorScan().scan());
    }
}
