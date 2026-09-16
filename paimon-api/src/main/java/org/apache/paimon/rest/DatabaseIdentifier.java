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

package org.apache.paimon.rest;

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.catalog.Identifier;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A REST database name and its optional database-level branch or immutable tag selector. */
@Experimental
public final class DatabaseIdentifier {

    private static final String BRANCH_SUFFIX = "$branch_";
    private static final String TAG_SUFFIX = "$tag_";

    private final String databaseName;
    @Nullable private final DatabaseReference reference;

    private DatabaseIdentifier(String databaseName, @Nullable DatabaseReference reference) {
        this.databaseName = databaseName;
        this.reference = reference;
    }

    /**
     * Parses a decoded database name, such as {@code training$branch_experiment}.
     *
     * <p>The suffixes {@code $branch_} and {@code $tag_} are reserved. A name containing either
     * marker must have exactly one valid reference suffix. Other dollar signs remain literal.
     * Callers retain the original name in table identifiers and encode it as one REST path segment.
     */
    public static DatabaseIdentifier parse(String name) {
        checkArgument(name != null && !name.trim().isEmpty(), "Database name must not be blank");
        int branch = name.indexOf(BRANCH_SUFFIX);
        int tag = name.indexOf(TAG_SUFFIX);
        if (branch < 0 && tag < 0) {
            return new DatabaseIdentifier(name, null);
        }
        boolean isBranch = branch >= 0 && (tag < 0 || branch < tag);
        int separator = isBranch ? branch : tag;
        String database = name.substring(0, separator);
        checkArgument(!database.trim().isEmpty(), "Database name must not be blank");
        String reference =
                name.substring(
                        separator + (isBranch ? BRANCH_SUFFIX.length() : TAG_SUFFIX.length()));
        return new DatabaseIdentifier(
                database,
                new DatabaseReference(
                        isBranch ? DatabaseReferenceType.BRANCH : DatabaseReferenceType.TAG,
                        reference));
    }

    /** The physical database name, without the reference suffix. */
    public String getDatabaseName() {
        return databaseName;
    }

    @Nullable
    public DatabaseReference getReference() {
        return reference;
    }

    static void checkNoReference(String database, String operation) {
        if (parse(database).getReference() != null) {
            throw new UnsupportedOperationException(
                    operation + " does not support database reference suffixes: " + database);
        }
    }

    static void checkTableName(String database, String table) {
        checkArgument(
                parse(database).getReference() == null
                        || Identifier.create(database, table).getBranchName() == null,
                "Table branch suffixes cannot be combined with a database reference");
    }
}
