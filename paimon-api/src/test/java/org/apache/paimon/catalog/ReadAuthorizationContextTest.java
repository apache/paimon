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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ReadAuthorizationContext}. */
class ReadAuthorizationContextTest {

    private static final Identifier ROOT = Identifier.create("db", "view");
    private static final Identifier TABLE = Identifier.create("db", "table");
    private static final Identifier SECOND_TABLE = Identifier.create("db", "second_table");

    private static ReadAuthorizationResource resource(Identifier table) {
        return ReadAuthorizationResource.table(table);
    }

    @Test
    void testPendingContextDoesNotAuthorizeByItself() {
        ReadAuthorizationContext context = ReadAuthorizationContext.forView(ROOT);

        assertThat(context.isDirect()).isFalse();
        assertThat(context.authorizationRootType()).contains(ReadAuthorizationRootType.VIEW);
        assertThat(context.authorizationRoot()).contains(ROOT);
        assertThat(context.readGrant()).isEmpty();
        assertThat(context.authorizedResources()).isEmpty();
        assertThat(context.authorizes(ReadAuthorizationRootType.TABLE, TABLE)).isFalse();
        assertThat(context.isExpired(0L)).isTrue();
    }

    @Test
    void testExtendAndSerializeLease() throws Exception {
        ReadAuthorizationContext context = ReadAuthorizationContext.forView(ROOT);
        context.updateGrant(Collections.singletonList(resource(TABLE)), "grant-1", 100L);
        context.updateGrant(Collections.singletonList(resource(SECOND_TABLE)), "grant-2", 200L);
        context.updateGrant(Collections.emptyList(), "grant-3", 300L);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(context);
        }
        ReadAuthorizationContext restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (ReadAuthorizationContext) input.readObject();
        }

        assertThat(restored).isEqualTo(context);
        assertThat(restored.authorizationRoot()).contains(ROOT);
        assertThat(restored.authorizedResources())
                .containsExactly(resource(TABLE), resource(SECOND_TABLE));
        assertThat(restored.authorizes(ReadAuthorizationRootType.TABLE, SECOND_TABLE)).isTrue();
        assertThat(restored.readGrant()).contains("grant-3");
        assertThat(restored.expiresAtMillis()).isEqualTo(300L);

        ReadAuthorizationContext tableContext =
                restored.forDependenciesOf(ReadAuthorizationRootType.TABLE, SECOND_TABLE);
        assertThat(tableContext.authorizationRoot()).contains(ROOT);
        assertThat(tableContext).isEqualTo(restored);
        tableContext.updateGrant(Collections.emptyList(), "branch-grant", 400L);
        assertThat(tableContext).isNotEqualTo(restored);
        assertThat(tableContext.readGrant()).contains("branch-grant");
        assertThat(restored.readGrant()).contains("grant-3");
    }

    @Test
    void testInitialUpdateRequiresDependency() {
        assertThatThrownBy(
                        () ->
                                ReadAuthorizationContext.forView(ROOT)
                                        .updateGrant(Collections.emptyList(), "new-grant", 200L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initial");
    }
}
