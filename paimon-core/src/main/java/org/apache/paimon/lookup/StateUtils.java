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

package org.apache.paimon.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

/** Utilities shared by lookup state implementations. */
public final class StateUtils {

    public static BinaryExternalSortBuffer createBulkLoadSorter(
            IOManager ioManager, CoreOptions options) {
        return BinaryExternalSortBuffer.create(
                ioManager,
                RowType.of(DataTypes.BYTES(), DataTypes.BYTES()),
                new int[] {0},
                options.writeBufferSize() / 2,
                options.pageSize(),
                options.localSortMaxNumFileHandles(),
                options.spillCompressOptions(),
                options.writeBufferSpillDiskSize());
    }

    private StateUtils() {}
}
