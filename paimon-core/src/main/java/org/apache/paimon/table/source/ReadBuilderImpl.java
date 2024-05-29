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

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.TypeUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/** Implementation for {@link ReadBuilder}. */
public class ReadBuilderImpl implements ReadBuilder {

    private static final long serialVersionUID = 1L;

    private final InnerTable table;

    private Predicate filter;
    private int[][] projection;

    private Integer limit = null;

    private Map<String, String> partitionSpec;

    public ReadBuilderImpl(InnerTable table) {
        this.table = table;
    }

    @Override
    public String tableName() {
        return table.name();
    }

    @Override
    public RowType readType() {
        if (projection == null) {
            return table.rowType();
        }
        return TypeUtils.project(table.rowType(), Projection.of(projection).toTopLevelIndexes());
    }

    @Override
    public ReadBuilder withFilter(Predicate filter) {
        if (this.filter == null) {
            this.filter = filter;
        } else {
            this.filter = PredicateBuilder.and(this.filter, filter);
        }
        return this;
    }

    @Override
    public ReadBuilder withPartitionFilter(Map<String, String> partitionSpec) {
        this.partitionSpec = partitionSpec;
        return this;
    }

    @Override
    public ReadBuilder withProjection(int[][] projection) {
        this.projection = projection;
        return this;
    }

    @Override
    public ReadBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        return null;
    }

    @Override
    public TableScan newScan() {
        InnerTableScan tableScan =
                table.newScan().withFilter(filter).withPartitionFilter(partitionSpec);
        if (limit != null) {
            tableScan.withLimit(limit);
        }
        return tableScan;
    }

    @Override
    public StreamTableScan newStreamScan() {
        return (StreamTableScan) table.newStreamScan().withFilter(filter);
    }

    @Override
    public TableRead newRead() {
        InnerTableRead read = table.newRead().withFilter(filter);
        if (projection != null) {
            read.withProjection(projection);
        }
        return read;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReadBuilderImpl that = (ReadBuilderImpl) o;
        return Objects.equals(table.name(), that.table.name())
                && Objects.equals(filter, that.filter)
                && Arrays.deepEquals(projection, that.projection);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(table.name(), filter);
        result = 31 * result + Arrays.deepHashCode(projection);
        return result;
    }
}
