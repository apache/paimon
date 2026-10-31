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

package org.apache.paimon.catalog;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Identifier}. */
class IdentifierTest {

    @Test
    void testRejectBlankEncodedBranch() {
        for (String object :
                Arrays.asList("t$branch_", "t$branch_ ", "t$branch_\u3000", "t$branch_$options")) {
            Identifier identifier = Identifier.create("db", object);
            assertThatThrownBy(identifier::getBranchName)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Branch name cannot be blank, but this is: " + object);
            assertThatThrownBy(identifier::getTableName)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void testBlankBranchIsMainBranch() {
        for (String branch : Arrays.asList("", " ", "\t", "\u3000")) {
            Identifier identifier = new Identifier("db", "t", branch);
            assertThat(identifier).isEqualTo(Identifier.create("db", "t"));
            assertThat(identifier.getBranchName()).isNull();
            assertThat(new Identifier("db", "t", branch, "options").getObjectName())
                    .isEqualTo("t$options");
        }
    }

    @Test
    void testNonWhitespaceBranchIsNotBlank() {
        // Character.isWhitespace does not take these for whitespace, nor U+180E after Java 8.
        for (String branch : Arrays.asList("\u00A0", "\u2007", "\u202F", "\u0085", "\u180E")) {
            assertThat(Identifier.isBlankBranch(branch)).isFalse();
            Identifier identifier = new Identifier("db", "t", branch);
            assertThat(identifier.getObjectName()).isEqualTo("t$branch_" + branch);
            assertThat(identifier.getBranchName()).isEqualTo(branch);
            assertThat(Identifier.create("db", "t$branch_" + branch).getBranchName())
                    .isEqualTo(branch);
        }
    }
}
