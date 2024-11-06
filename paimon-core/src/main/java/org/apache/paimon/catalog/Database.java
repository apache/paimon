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

import org.apache.paimon.annotation.Public;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Interface of a database in a catalog.
 *
 * @since 1.0
 */
@Public
public interface Database {

    /** A name to identify this database. */
    String name();

    /** Options of this database. */
    Map<String, String> options();

    /** Optional comment of this database. */
    Optional<String> comment();

    static Database of(String name, Map<String, String> options, @Nullable String comment) {
        return new DatabaseImpl(name, options, comment);
    }

    static Database of(String name) {
        return new DatabaseImpl(name, new HashMap<>(), null);
    }

    /** Implementation of {@link Database}. */
    class DatabaseImpl implements Database {

        private final String name;
        private final Map<String, String> options;
        @Nullable private final String comment;

        public DatabaseImpl(String name, Map<String, String> options, @Nullable String comment) {
            this.name = name;
            this.options = options;
            this.comment = comment;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Map<String, String> options() {
            return options;
        }

        @Override
        public Optional<String> comment() {
            return Optional.ofNullable(comment);
        }
    }
}
