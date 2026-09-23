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

package org.apache.paimon.function;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FunctionChange}. */
public class FunctionChangeTest {

    @Test
    void testUpdateDefinitionHashCodeIncludesName() {
        FunctionDefinition definition = FunctionDefinition.sql("SELECT 1");
        FunctionChange a = FunctionChange.updateDefinition("f1", definition);
        FunctionChange b = FunctionChange.updateDefinition("f2", definition);

        assertThat(a).isNotEqualTo(b);
        assertThat(a.hashCode()).isNotEqualTo(b.hashCode());
    }

    @Test
    void testUpdateDefinitionEqualsHashCodeConsistency() {
        FunctionDefinition definition = FunctionDefinition.sql("SELECT 1");
        FunctionChange a = FunctionChange.updateDefinition("f1", definition);
        FunctionChange b =
                FunctionChange.updateDefinition("f1", FunctionDefinition.sql("SELECT 1"));

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void testAddDefinitionHashCodeIncludesName() {
        FunctionDefinition definition = FunctionDefinition.sql("SELECT 1");
        FunctionChange a = FunctionChange.addDefinition("f1", definition);
        FunctionChange b = FunctionChange.addDefinition("f2", definition);

        assertThat(a).isNotEqualTo(b);
        assertThat(a.hashCode()).isNotEqualTo(b.hashCode());
    }
}
