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

package org.apache.paimon.data.shredding;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.shredding.ShreddingWritePlanFactory;
import org.apache.paimon.types.RowType;

import java.util.List;

/** Creates per-file shared-shredding MAP write plans. */
public class MapSharedShreddingWritePlanFactory implements ShreddingWritePlanFactory {

    private final RowType logicalRowType;
    private final MapSharedShreddingContext context;

    public MapSharedShreddingWritePlanFactory(
            RowType logicalRowType, MapSharedShreddingContext context) {
        this.logicalRowType = logicalRowType;
        this.context = context;
    }

    @Override
    public RowType logicalRowType() {
        return logicalRowType;
    }

    @Override
    public boolean shouldCreateWritePlan() {
        return !context.isEmpty();
    }

    @Override
    public boolean shouldInferWritePlan() {
        return false;
    }

    @Override
    public int inferBufferRowCount() {
        return 0;
    }

    @Override
    public ShreddingWritePlan createWritePlan(List<InternalRow> sampleRows) {
        return new MapSharedShreddingWritePlan(logicalRowType, context.computeNextK(), context);
    }
}
