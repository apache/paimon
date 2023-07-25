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

package org.apache.paimon.types;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ReassignFieldId}. */
public class ReassignFieldIdTest {
    @Test
    void testReassignNestRowType() {
        RowType rowType =
                RowType.builder()
                        .field(
                                "a",
                                RowType.builder()
                                        .field(
                                                "b",
                                                RowType.builder()
                                                        .field("c", DataTypes.INT())
                                                        .build())
                                        .build())
                        .field("d", DataTypes.INT())
                        .build();
        ReassignFieldId reassignFieldId = new ReassignFieldId(new AtomicInteger(2));
        DataType resultDataType = reassignFieldId.visit(rowType);

        Set<Integer> fieldIds = new HashSet<>();
        resultDataType.collectFieldIds(fieldIds);
        assertThat(fieldIds).isEqualTo(new HashSet<>(Arrays.asList(3, 4, 5, 6)));
    }
}
