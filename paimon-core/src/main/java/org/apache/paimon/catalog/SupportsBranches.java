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

import javax.annotation.Nullable;

import java.util.List;

/** A {@link Catalog} supports creating and dropping table branches. */
public interface SupportsBranches extends Catalog {

    /**
     * Create a new branch for this table. By default, an empty branch will be created using the
     * latest schema. If you provide {@code #fromTag}, a branch will be created from the tag and the
     * data files will be inherited from it.
     *
     * @param identifier path of the table, cannot be system or branch name.
     * @param branch the branch name
     * @param fromTag from the tag
     * @throws TableNotExistException if the table in identifier doesn't exist
     * @throws DatabaseNotExistException if the database in identifier doesn't exist
     */
    void createBranch(Identifier identifier, String branch, @Nullable String fromTag)
            throws TableNotExistException, DatabaseNotExistException;

    /**
     * Drop the branch for this table.
     *
     * @param identifier path of the table, cannot be system or branch name.
     * @param branch the branch name
     * @throws TableNotExistException if the table in identifier doesn't exist
     * @throws DatabaseNotExistException if the database in identifier doesn't exist
     */
    void dropBranch(Identifier identifier, String branch)
            throws TableNotExistException, DatabaseNotExistException;

    /**
     * Fast-forward a branch to main branch.
     *
     * @param identifier path of the table, cannot be system or branch name.
     * @param branch the branch name
     * @throws TableNotExistException if the table in identifier doesn't exist
     * @throws DatabaseNotExistException if the database in identifier doesn't exist
     */
    void fastForward(Identifier identifier, String branch)
            throws TableNotExistException, DatabaseNotExistException;

    /**
     * List all branches of the table.
     *
     * @param identifier path of the table, cannot be system or branch name.
     * @throws TableNotExistException if the table in identifier doesn't exist
     * @throws DatabaseNotExistException if the database in identifier doesn't exist
     */
    List<String> listBranches(Identifier identifier)
            throws TableNotExistException, DatabaseNotExistException;
}
