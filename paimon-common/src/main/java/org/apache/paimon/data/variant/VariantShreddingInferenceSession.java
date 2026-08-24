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

package org.apache.paimon.data.variant;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Rolling-writer-scoped Variant inference state.
 *
 * <p>The state is intentionally not persisted or shared between rolling writers.
 */
public class VariantShreddingInferenceSession {

    private final InferVariantShreddingSchema inferrer;
    private final int effectiveSampleSize;
    private final double admissionRatio;
    private final double retentionRatio;

    private InferVariantShreddingSchema.InferenceEvidence committedEvidence;
    private Map<List<Integer>, DataType> committedSelectedSchemas = Collections.emptyMap();
    private InferVariantShreddingSchema.AdaptiveInferenceResult pendingResult;

    public VariantShreddingInferenceSession(
            InferVariantShreddingSchema inferrer,
            int effectiveSampleSize,
            double admissionRatio,
            double retentionRatio) {
        Preconditions.checkArgument(
                effectiveSampleSize > 0, "Effective sample size must be positive.");
        Preconditions.checkArgument(
                admissionRatio >= 0 && admissionRatio <= 1,
                "Admission ratio must be between 0 and 1.");
        Preconditions.checkArgument(
                retentionRatio >= 0 && retentionRatio <= admissionRatio,
                "Retention ratio must be between 0 and the admission ratio.");
        this.inferrer = inferrer;
        this.effectiveSampleSize = effectiveSampleSize;
        this.admissionRatio = admissionRatio;
        this.retentionRatio = retentionRatio;
    }

    public boolean hasPrior() {
        return committedEvidence != null;
    }

    public RowType inferSchema(List<InternalRow> rows) {
        if (committedEvidence == null) {
            pendingResult = inferrer.inferInitial(rows, effectiveSampleSize);
        } else {
            pendingResult =
                    inferrer.inferAdaptive(
                            committedEvidence,
                            committedSelectedSchemas,
                            rows,
                            effectiveSampleSize,
                            admissionRatio,
                            retentionRatio);
        }
        return pendingResult.physicalRowType();
    }

    public void commitPendingInference() {
        Preconditions.checkState(pendingResult != null, "No pending Variant inference to commit.");
        committedEvidence = pendingResult.evidence();
        committedSelectedSchemas = pendingResult.selectedSchemas();
        pendingResult = null;
    }
}
