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

package org.apache.paimon.lineage;

import org.apache.paimon.predicate.Predicate;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Iterator;

/** Metadata store will manage table lineage and data lineage information for the catalog. */
public interface LineageMeta extends Serializable {
    /**
     * Store the source table and job lineage.
     *
     * @param entity the table lineage entity
     */
    void storeSourceTableLineage(TableLineageEntity entity);

    /**
     * Delete the source table lineage for given job.
     *
     * @param job the job for table lineage
     */
    void deleteSourceTableLineage(String job);

    /**
     * Get source table and job lineages.
     *
     * @param predicate the predicate for the table lineages
     * @return the iterator for source table and job lineages
     */
    Iterator<TableLineageEntity> sourceTableLineages(@Nullable Predicate predicate);

    /**
     * Store the sink table and job lineage.
     *
     * @param entity the table lineage entity
     */
    void storeSinkTableLineage(TableLineageEntity entity);

    /**
     * Get sink table and job lineages.
     *
     * @param predicate the predicate for the table lineages
     * @return the iterator for source table and job lineages
     */
    Iterator<TableLineageEntity> sinkTableLineages(@Nullable Predicate predicate);

    /**
     * Delete the sink table lineage for given job.
     *
     * @param job the job for table lineage
     */
    void deleteSinkTableLineage(String job);

    /**
     * Store the source table and job lineage.
     *
     * @param entity the data lineage entity
     */
    void storeSourceDataLineage(DataLineageEntity entity);

    /**
     * Get source data and job lineages.
     *
     * @param predicate the predicate for the table lineages
     * @return the iterator for source table and job lineages
     */
    Iterator<DataLineageEntity> sourceDataLineages(@Nullable Predicate predicate);

    /**
     * Store the source table and job lineage.
     *
     * @param entity the data lineage entity
     */
    void storeSinkDataLineage(DataLineageEntity entity);

    /**
     * Get sink data and job lineages.
     *
     * @param predicate the predicate for the table lineages
     * @return the iterator for source table and job lineages
     */
    Iterator<DataLineageEntity> sinkDataLineages(@Nullable Predicate predicate);
}
