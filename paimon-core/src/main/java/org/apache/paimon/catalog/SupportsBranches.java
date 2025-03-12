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
     * @throws BranchAlreadyExistException if the branch already exists
     * @throws TagNotExistException if the tag doesn't exist
     */
    void createBranch(Identifier identifier, String branch, @Nullable String fromTag)
            throws TableNotExistException, BranchAlreadyExistException, TagNotExistException;

    /**
     * Drop the branch for this table.
     *
     * @param identifier path of the table, cannot be system or branch name.
     * @param branch the branch name
     * @throws BranchNotExistException if the branch doesn't exist
     */
    void dropBranch(Identifier identifier, String branch) throws BranchNotExistException;

    /**
     * Fast-forward a branch to main branch.
     *
     * @param identifier path of the table, cannot be system or branch name.
     * @param branch the branch name
     * @throws BranchNotExistException if the branch doesn't exist
     */
    void fastForward(Identifier identifier, String branch) throws BranchNotExistException;

    /**
     * List all branches of the table.
     *
     * @param identifier path of the table, cannot be system or branch name.
     * @throws TableNotExistException if the table in identifier doesn't exist
     */
    List<String> listBranches(Identifier identifier) throws TableNotExistException;

    /** Exception for trying to create a branch that already exists. */
    class BranchAlreadyExistException extends Exception {

        private static final String MSG = "Branch %s in table %s already exists.";

        private final Identifier identifier;
        private final String branch;

        public BranchAlreadyExistException(Identifier identifier, String branch) {
            this(identifier, branch, null);
        }

        public BranchAlreadyExistException(Identifier identifier, String branch, Throwable cause) {
            super(String.format(MSG, branch, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.branch = branch;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String branch() {
            return branch;
        }
    }

    /** Exception for trying to operate on a branch that doesn't exist. */
    class BranchNotExistException extends Exception {

        private static final String MSG = "Branch %s in table %s doesn't exist.";

        private final Identifier identifier;
        private final String branch;

        public BranchNotExistException(Identifier identifier, String branch) {
            this(identifier, branch, null);
        }

        public BranchNotExistException(Identifier identifier, String branch, Throwable cause) {
            super(String.format(MSG, branch, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.branch = branch;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String branch() {
            return branch;
        }
    }

    /** Exception for trying to operate on a tag that doesn't exist. */
    class TagNotExistException extends Exception {

        private static final String MSG = "Tag %s in table %s doesn't exist.";

        private final Identifier identifier;
        private final String tag;

        public TagNotExistException(Identifier identifier, String tag) {
            this(identifier, tag, null);
        }

        public TagNotExistException(Identifier identifier, String tag, Throwable cause) {
            super(String.format(MSG, tag, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.tag = tag;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String tag() {
            return tag;
        }
    }
}
