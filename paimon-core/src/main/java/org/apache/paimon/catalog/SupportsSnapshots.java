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

import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.table.TableSnapshot;

import java.util.List;
import java.util.Optional;

/** A {@link Catalog} supports committing and loading table snapshots. */
public interface SupportsSnapshots extends Catalog {

    /**
     * Commit the {@link Snapshot} for table identified by the given {@link Identifier}.
     *
     * @param identifier Path of the table
     * @param snapshot Snapshot to be committed
     * @param statistics statistics information of this change
     * @return Success or not
     * @throws Catalog.TableNotExistException if the target does not exist
     */
    boolean commitSnapshot(Identifier identifier, Snapshot snapshot, List<Partition> statistics)
            throws Catalog.TableNotExistException;

    /**
     * Return the snapshot of table identified by the given {@link Identifier}.
     *
     * @param identifier Path of the table
     * @return The requested snapshot of the table
     * @throws Catalog.TableNotExistException if the target does not exist
     */
    Optional<TableSnapshot> loadSnapshot(Identifier identifier)
            throws Catalog.TableNotExistException;
}
