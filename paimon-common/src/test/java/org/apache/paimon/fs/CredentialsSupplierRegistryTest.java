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

package org.apache.paimon.fs;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CredentialsSupplierRegistry}. */
class CredentialsSupplierRegistryTest {

    @Test
    void testSupplierStaysWhileReferencedAndGoesAwayAfterwards() throws InterruptedException {
        Supplier<Map<String, String>> held = () -> Collections.singletonMap("k", "v");
        String heldId = CredentialsSupplierRegistry.register(held);
        String droppedId = registerUnreferenced();

        for (int i = 0; i < 100 && CredentialsSupplierRegistry.get(droppedId) != null; i++) {
            System.gc();
            Thread.sleep(10);
        }

        assertThat(CredentialsSupplierRegistry.get(droppedId)).isNull();
        assertThat(CredentialsSupplierRegistry.get(heldId)).isSameAs(held);
    }

    private static String registerUnreferenced() {
        // a capturing lambda, since a non-capturing one is a cached singleton that is never freed
        String value = String.valueOf(System.nanoTime());
        return CredentialsSupplierRegistry.register(() -> Collections.singletonMap("k", value));
    }
}
