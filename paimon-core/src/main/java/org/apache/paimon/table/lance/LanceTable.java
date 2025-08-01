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

package org.apache.paimon.table.lance;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import java.util.Map;

/**
 * A lance table format table, paimon does not support read and write operation on this table yet.
 */
public interface LanceTable extends Table {

    /** Object location in file system. */
    String location();

    @Override
    LanceTable copy(Map<String, String> dynamicOptions);

    /** Create a new builder for {@link LanceTable}. */
    static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link LanceTable}. */
    class Builder {

        private Identifier identifier;
        private FileIO fileIO;
        private RowType rowType;
        private String location;
        private Map<String, String> options;
        private String comment;

        public Builder identifier(Identifier identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder fileIO(FileIO fileIO) {
            this.fileIO = fileIO;
            return this;
        }

        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public Builder location(String location) {
            this.location = location;
            return this;
        }

        public Builder options(Map<String, String> options) {
            this.options = options;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public LanceTable build() {
            return new LanceTableImpl(identifier, fileIO, rowType, location, options, comment);
        }
    }
}
