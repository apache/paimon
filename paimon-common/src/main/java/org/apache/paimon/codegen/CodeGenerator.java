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

package org.apache.paimon.codegen;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.List;

/** {@link GeneratedClass} generator. */
public interface CodeGenerator {

    GeneratedClass<Projection> generateProjection(
            String name, RowType inputType, int[] inputMapping);

    /**
     * Generate a {@link NormalizedKeyComputer}.
     *
     * @param fieldTypes Both the input row field types and the sort key field types. Records are
     *     compared by the first field, then the second field, then the third field and so on. All
     *     fields are compared in ascending order.
     */
    GeneratedClass<NormalizedKeyComputer> generateNormalizedKeyComputer(
            List<DataType> fieldTypes, String name);

    /**
     * Generate a {@link RecordComparator}.
     *
     * @param fieldTypes Both the input row field types and the sort key field types. Records are *
     *     compared by the first field, then the second field, then the third field and so on. All *
     *     fields are compared in ascending order.
     */
    GeneratedClass<RecordComparator> generateRecordComparator(
            List<DataType> fieldTypes, String name);
}
