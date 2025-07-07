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

package org.apache.paimon.table.object;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.Map;

/**
 * An object table refers to a directory that contains multiple objects (files). Object table
 * provides metadata indexes for unstructured data objects in this directory, allowing users to
 * analyze unstructured data in Object Storage.
 */
public interface ObjectTable extends Table {

    RowType SCHEMA =
            RowType.builder()
                    .field("relative_path", DataTypes.STRING().notNull())
                    .field("name", DataTypes.STRING().notNull())
                    .field("length", DataTypes.BIGINT().notNull())
                    .field("mtime", DataTypes.BIGINT().notNull())
                    .field("atime", DataTypes.BIGINT().notNull())
                    .field("owner", DataTypes.STRING().nullable())
                    .build()
                    .notNull();

    /** Object location in file system. */
    String location();

    @Override
    ObjectTable copy(Map<String, String> dynamicOptions);

    /** Create a new builder for {@link ObjectTable}. */
    static ObjectTable.Builder builder() {
        return new ObjectTable.Builder();
    }

    /** Builder for {@link ObjectTable}. */
    class Builder {

        private Identifier identifier;
        private FileIO fileIO;
        private String location;
        private String comment;

        public ObjectTable.Builder identifier(Identifier identifier) {
            this.identifier = identifier;
            return this;
        }

        public ObjectTable.Builder fileIO(FileIO fileIO) {
            this.fileIO = fileIO;
            return this;
        }

        public ObjectTable.Builder location(String location) {
            this.location = location;
            return this;
        }

        public ObjectTable.Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public ObjectTable build() {
            return new ObjectTableImpl(identifier, fileIO, location, comment);
        }
    }
}
