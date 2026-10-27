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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Projects whole-record DELETE rows to fields required by data files. */
final class DeleteRowFieldsProjector {

    private final ProjectedRow projectedValue;
    private final KeyValue reused = new KeyValue();

    static @Nullable int[] createProjection(TableSchema schema, CoreOptions options) {
        if (!options.deleteRowThinMode()
                || schema.primaryKeys().isEmpty()
                || !removeWholeRecordOnDelete(options)) {
            return null;
        }

        Set<String> requiredFields = new HashSet<>(schema.primaryKeys());
        requiredFields.addAll(options.sequenceField());
        if (options.recordLevelTimeField() != null) {
            requiredFields.add(options.recordLevelTimeField());
        }
        requiredFields.addAll(options.clusteringColumns());
        for (DataField field : schema.fields()) {
            if (!field.type().isNullable()) {
                requiredFields.add(field.name());
            }
        }

        int[] projection = new int[schema.fields().size()];
        Arrays.fill(projection, -1);
        for (int i = 0; i < schema.fields().size(); i++) {
            if (requiredFields.contains(schema.fields().get(i).name())) {
                projection[i] = i;
            }
        }
        return Arrays.stream(projection).anyMatch(field -> field < 0) ? projection : null;
    }

    private static boolean removeWholeRecordOnDelete(CoreOptions options) {
        switch (options.mergeEngine()) {
            case DEDUPLICATE:
                return true;
            case PARTIAL_UPDATE:
                return options.partialUpdateRemoveRecordOnDelete();
            case AGGREGATE:
                return options.aggregationRemoveRecordOnDelete();
            default:
                return false;
        }
    }

    DeleteRowFieldsProjector(int[] projection) {
        this.projectedValue = ProjectedRow.from(projection);
    }

    KeyValue project(KeyValue kv) {
        return reused.replace(
                        kv.key(),
                        kv.sequenceNumber(),
                        kv.valueKind(),
                        projectedValue.replaceRow(kv.value()))
                .setLevel(kv.level());
    }
}
