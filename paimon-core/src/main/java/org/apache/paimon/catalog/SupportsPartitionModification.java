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

import org.apache.paimon.partition.PartitionStatistics;

import java.util.List;
import java.util.Map;

/** A {@link Catalog} supports modifying table partitions. */
public interface SupportsPartitionModification extends Catalog {

    /**
     * Create partitions of the specify table. Ignore existing partitions.
     *
     * @param identifier path of the table to create partitions
     * @param partitions partitions to be created
     * @throws TableNotExistException if the table does not exist
     */
    void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException;

    /**
     * Drop partitions of the specify table. Ignore non-existent partitions.
     *
     * @param identifier path of the table to drop partitions
     * @param partitions partitions to be deleted
     * @throws TableNotExistException if the table does not exist
     */
    void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException;

    /**
     * Alter partitions of the specify table. For non-existent partitions, partitions will be
     * created directly.
     *
     * @param identifier path of the table to alter partitions
     * @param partitions partitions to be altered
     * @throws TableNotExistException if the table does not exist
     */
    void alterPartitions(Identifier identifier, List<PartitionStatistics> partitions)
            throws TableNotExistException;
}
