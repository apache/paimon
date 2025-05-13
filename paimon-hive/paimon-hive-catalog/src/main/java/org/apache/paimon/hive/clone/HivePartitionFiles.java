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

package org.apache.paimon.hive.clone;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;

import java.io.Serializable;
import java.util.List;

/** Files grouped by partition with necessary information. */
public class HivePartitionFiles implements Serializable {

    private static final long serialVersionUID = 1L;

    private final BinaryRow partition;
    private final List<Path> paths;
    private final List<Long> fileSizes;
    private final String format;

    public HivePartitionFiles(
            BinaryRow partition, List<Path> paths, List<Long> fileSizes, String format) {
        this.partition = partition;
        this.paths = paths;
        this.fileSizes = fileSizes;
        this.format = format;
    }

    public BinaryRow partition() {
        return partition;
    }

    public List<Path> paths() {
        return paths;
    }

    public List<Long> fileSizes() {
        return fileSizes;
    }

    public String format() {
        return format;
    }
}
