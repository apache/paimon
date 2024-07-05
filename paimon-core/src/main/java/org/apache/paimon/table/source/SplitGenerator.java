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

package org.apache.paimon.table.source;

import org.apache.paimon.io.DataFileMeta;

import java.util.List;

/** Generate splits from {@link DataFileMeta}s. */
public interface SplitGenerator {

    boolean alwaysRawConvertible();

    List<SplitGroup> splitForBatch(List<DataFileMeta> files);

    List<SplitGroup> splitForStreaming(List<DataFileMeta> files);

    /** Split group. */
    class SplitGroup {

        public final List<DataFileMeta> files;
        public final boolean rawConvertible;

        private SplitGroup(List<DataFileMeta> files, boolean rawConvertible) {
            this.files = files;
            this.rawConvertible = rawConvertible;
        }

        public static SplitGroup rawConvertibleGroup(List<DataFileMeta> files) {
            return new SplitGroup(files, true);
        }

        public static SplitGroup nonRawConvertibleGroup(List<DataFileMeta> files) {
            return new SplitGroup(files, false);
        }
    }
}
