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

package org.apache.paimon.rest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the reserved database reference suffix grammar independently of HTTP encoding. */
class DatabaseIdentifierTest {

    @Test
    void testBranchAndTagSelectors() {
        DatabaseIdentifier branch = DatabaseIdentifier.parse("training db$branch_experiment");
        assertThat(branch.getDatabaseName()).isEqualTo("training db");
        assertThat(branch.getReference())
                .isEqualTo(new DatabaseReference(DatabaseReferenceType.BRANCH, "experiment"));

        DatabaseIdentifier tag = DatabaseIdentifier.parse("training$literal$tag_train_v1");
        assertThat(tag.getDatabaseName()).isEqualTo("training$literal");
        assertThat(tag.getReference())
                .isEqualTo(new DatabaseReference(DatabaseReferenceType.TAG, "train_v1"));

        assertThat(DatabaseIdentifier.parse("training$branch_123").getReference())
                .isEqualTo(new DatabaseReference(DatabaseReferenceType.BRANCH, "123"));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "training",
                "training$literal",
                "training$Branch_main",
                "training%24branch_main"
            })
    void testOrdinaryNamesRemainLiteral(String name) {
        DatabaseIdentifier identifier = DatabaseIdentifier.parse(name);
        assertThat(identifier.getDatabaseName()).isEqualTo(name);
        assertThat(identifier.getReference()).isNull();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(
            strings = {
                " ",
                "$branch_main",
                "$tag_v1",
                "training$branch_",
                "training$tag_",
                "training$branch_a/b",
                "training$tag_..",
                "training$branch_a$branch_b",
                "training$branch_a$tag_b",
                "training$tag_a$branch_b"
            })
    void testMalformedSelectorsAreNotLiteralDatabaseNames(String name) {
        assertThatThrownBy(() -> DatabaseIdentifier.parse(name))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
