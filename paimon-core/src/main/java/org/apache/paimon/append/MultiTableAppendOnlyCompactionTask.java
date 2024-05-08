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

package org.apache.paimon.append;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;

import java.util.List;
import java.util.Objects;

/** Compaction task for multi table . */
public class MultiTableAppendOnlyCompactionTask extends AppendOnlyCompactionTask {
    private final Identifier tableIdentifier;

    public MultiTableAppendOnlyCompactionTask(
            BinaryRow partition, List<DataFileMeta> files, Identifier identifier) {
        super(partition, files);
        this.tableIdentifier = identifier;
    }

    public Identifier tableIdentifier() {
        return tableIdentifier;
    }

    public int hashCode() {
        return Objects.hash(partition(), compactBefore(), compactAfter(), tableIdentifier);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MultiTableAppendOnlyCompactionTask that = (MultiTableAppendOnlyCompactionTask) o;
        return Objects.equals(partition(), that.partition())
                && Objects.equals(compactBefore(), that.compactBefore())
                && Objects.equals(compactAfter(), that.compactAfter())
                && Objects.equals(tableIdentifier, that.tableIdentifier);
    }
}
