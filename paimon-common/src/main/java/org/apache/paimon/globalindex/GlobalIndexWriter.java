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

package org.apache.paimon.globalindex;

import javax.annotation.Nullable;

import java.util.List;

/** Index writer for global index. */
public interface GlobalIndexWriter {

    void write(Object key);

    List<ResultEntry> finish();

    /** Write result meta. */
    class ResultEntry {
        private final String fileName;
        @Nullable private final String meta;

        public ResultEntry(String fileName, @Nullable String meta) {
            this.fileName = fileName;
            this.meta = meta;
        }

        public String fileName() {
            return fileName;
        }

        public String meta() {
            return meta;
        }

        public static ResultEntry of(String fileName, String meta) {
            return new ResultEntry(fileName, meta);
        }
    }
}
