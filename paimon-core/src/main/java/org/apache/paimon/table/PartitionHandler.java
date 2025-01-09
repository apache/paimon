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

package org.apache.paimon.table;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;

import java.util.List;
import java.util.Map;

/** Handler to handle partitions. */
public interface PartitionHandler extends AutoCloseable {

    void createPartitions(List<Map<String, String>> partitions)
            throws Catalog.TableNotExistException;

    void dropPartitions(List<Map<String, String>> partitions) throws Catalog.TableNotExistException;

    void alterPartitions(List<Partition> partitions) throws Catalog.TableNotExistException;

    void markDonePartitions(List<Map<String, String>> partitions)
            throws Catalog.TableNotExistException;

    static PartitionHandler create(Catalog catalog, Identifier identifier) {
        return new PartitionHandler() {

            @Override
            public void createPartitions(List<Map<String, String>> partitions)
                    throws Catalog.TableNotExistException {
                catalog.createPartitions(identifier, partitions);
            }

            @Override
            public void dropPartitions(List<Map<String, String>> partitions)
                    throws Catalog.TableNotExistException {
                catalog.dropPartitions(identifier, partitions);
            }

            @Override
            public void alterPartitions(List<Partition> partitions)
                    throws Catalog.TableNotExistException {
                catalog.alterPartitions(identifier, partitions);
            }

            @Override
            public void markDonePartitions(List<Map<String, String>> partitions)
                    throws Catalog.TableNotExistException {
                catalog.markDonePartitions(identifier, partitions);
            }

            @Override
            public void close() throws Exception {
                catalog.close();
            }
        };
    }
}
