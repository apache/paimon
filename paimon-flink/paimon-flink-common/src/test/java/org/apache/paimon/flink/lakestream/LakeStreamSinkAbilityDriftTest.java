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

package org.apache.paimon.flink.lakestream;

import org.apache.paimon.flink.sink.FlinkTableSink;

import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsTruncate;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keeps the ability surface of {@link LakeStreamDynamicTableSink} in step with the sinks it stands
 * in front of.
 *
 * <p>Flink enables a statement by probing the sink for a {@code SupportsXxx} interface, so an
 * ability that is not exposed is a statement the table loses without anything saying so. When
 * Paimon or a store gains one, this test fails and the choice has to be made explicitly: offer it,
 * or record it in {@link #KNOWN_WITHHELD} with the reason it is deliberately not offered.
 */
public class LakeStreamSinkAbilityDriftTest {

    /** The abilities deliberately not offered, and why. An entry here is a decision, not a gap. */
    private static final Map<Class<?>, String> KNOWN_WITHHELD = new LinkedHashMap<>();

    static {
        KNOWN_WITHHELD.put(
                SupportsOverwrite.class,
                "INSERT OVERWRITE has to reach Paimon rather than the store, which is separate work");
        KNOWN_WITHHELD.put(
                SupportsDeletePushDown.class,
                "a pushed-down DELETE would only reach the rows the store happens to hold; "
                        + "withholding it leaves DELETE to the row-level path, which is refused");
        KNOWN_WITHHELD.put(
                SupportsTruncate.class,
                "TRUNCATE replaces the whole table, so it belongs with INSERT OVERWRITE");
    }

    @Test
    public void onlyTheKnownAbilitiesAreWithheld() {
        Set<Class<?>> delegates = abilitiesOf(FlinkTableSink.class);
        delegates.addAll(abilitiesOf(FakeStoreSink.class));
        Class<?> flussSink = flussSink();
        if (flussSink != null) {
            delegates.addAll(abilitiesOf(flussSink));
        }

        Set<Class<?>> offered = abilitiesOf(LakeStreamDynamicTableSink.class);
        Set<Class<?>> withheld = new HashSet<>(delegates);
        withheld.removeAll(offered);

        assertThat(withheld)
                .as(
                        "a sink behind the lake-stream path gained an ability that "
                                + "LakeStreamDynamicTableSink does not expose: offer it, or add it "
                                + "to KNOWN_WITHHELD with the reason it is deliberately not offered. "
                                + "Withheld now: %s",
                        describe(withheld))
                .isEqualTo(KNOWN_WITHHELD.keySet());

        assertThat(offered)
                .as("an ability recorded as withheld is offered now; drop it from KNOWN_WITHHELD")
                .doesNotContainAnyElementsOf(KNOWN_WITHHELD.keySet());
    }

    @Test
    public void flussIsTheOnlyStoreOnTheTestClasspath() {
        Set<String> stores = new HashSet<>();
        for (Factory factory : ServiceLoader.load(Factory.class, getClass().getClassLoader())) {
            String type = factory.getClass().getName();
            // a store is a third-party connector, so its factory sits outside both Paimon's own
            // packages (which include the test doubles) and Flink's built-in ones
            if (factory instanceof CatalogFactory
                    && !type.startsWith("org.apache.paimon.")
                    && !type.startsWith("org.apache.flink.")) {
                stores.add(type);
            }
        }

        // a Java 8 build gates the Fluss test dependencies out, so there is no real store there
        Set<String> expected =
                flussSink() == null
                        ? Collections.<String>emptySet()
                        : Collections.singleton(
                                "org.apache.fluss.flink.catalog.FlinkCatalogFactory");
        assertThat(stores)
                .as(
                        "a new Lakestream Store reached the test class path: add its sink to "
                                + "onlyTheKnownAbilitiesAreWithheld, so abilities that Fluss does "
                                + "not have but the new store does are covered as well")
                .isEqualTo(expected);
    }

    private static String describe(Set<Class<?>> abilities) {
        StringBuilder text = new StringBuilder();
        for (Class<?> ability : abilities) {
            if (text.length() > 0) {
                text.append(", ");
            }
            text.append(ability.getSimpleName());
            String reason = KNOWN_WITHHELD.get(ability);
            if (reason != null) {
                text.append(" (").append(reason).append(")");
            }
        }
        return text.toString();
    }

    private static Set<Class<?>> abilitiesOf(Class<?> type) {
        Set<Class<?>> abilities = new HashSet<>();
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            collectAbilities(c, abilities);
        }
        return abilities;
    }

    private static void collectAbilities(Class<?> type, Set<Class<?>> into) {
        for (Class<?> iface : type.getInterfaces()) {
            if (iface.getName().startsWith("org.apache.flink.table.connector.")
                    && iface.getName().contains(".abilities.")) {
                into.add(iface);
            }
            collectAbilities(iface, into);
        }
    }

    /** Reached by name so that Paimon compiles against no Fluss class. */
    @Nullable
    private static Class<?> flussSink() {
        try {
            return Class.forName("org.apache.fluss.flink.sink.FlinkTableSink");
        } catch (ClassNotFoundException e) {
            return null;
        }
    }
}
