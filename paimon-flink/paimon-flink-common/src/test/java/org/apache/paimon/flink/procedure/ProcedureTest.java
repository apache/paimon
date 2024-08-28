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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.flink.action.ActionFactory;

import org.apache.flink.table.annotation.ProcedureHint;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for procedures. */
public class ProcedureTest {
    @Test
    public void testProcedureCoverAllActions() {
        Set<String> expectedExclusions = new HashSet<>();
        // Can be covered by `DELETE FROM` syntax. No procedure needed.
        expectedExclusions.add("delete");
        List<String> actionIdentifiers =
                FactoryUtil.discoverIdentifiers(
                        ActionFactory.class.getClassLoader(), ActionFactory.class);
        assertThat(actionIdentifiers.size()).isNotZero();
        for (String identifier : actionIdentifiers) {
            if (expectedExclusions.contains(identifier)) {
                assertThatThrownBy(
                                () ->
                                        FactoryUtil.discoverFactory(
                                                ProcedureBase.class.getClassLoader(),
                                                ProcedureBase.class,
                                                identifier))
                        .isInstanceOf(FactoryException.class)
                        .hasMessageContaining("Could not find any factory for identifier");
            } else {
                ProcedureBase procedure =
                        FactoryUtil.discoverFactory(
                                ProcedureBase.class.getClassLoader(),
                                ProcedureBase.class,
                                identifier);
                assertThat(procedure).isNotNull();
            }
        }
    }

    @Test
    public void testProcedureHasNamedArgument() {
        Set<String> expectedExclusions = new HashSet<>();
        expectedExclusions.add("drop_partition"); // Has been deprecated.
        // The followings involve compatibility issue to be resolved in followup PR
        expectedExclusions.add("merge_into");
        expectedExclusions.add("rollback_to");
        expectedExclusions.add("mark_partition_done");

        List<String> identifiers =
                FactoryUtil.discoverIdentifiers(
                        ProcedureBase.class.getClassLoader(), ProcedureBase.class);
        assertThat(identifiers.size()).isNotZero();
        for (String identifier : identifiers) {
            ProcedureBase procedure =
                    FactoryUtil.discoverFactory(
                            ProcedureBase.class.getClassLoader(), ProcedureBase.class, identifier);
            Method method = getMethodFromName(procedure.getClass(), "call");
            if (expectedExclusions.contains(identifier)) {
                assertThat(method)
                        .matches(
                                x -> !x.isAnnotationPresent(ProcedureHint.class),
                                String.format(
                                        "Procedure %s has supported named argument. Should be removed from exclusion list.",
                                        procedure.identifier()));
            } else {
                assertThat(method)
                        .matches(
                                x -> x.isAnnotationPresent(ProcedureHint.class),
                                String.format(
                                        "Procedure %s should have its call method decorated by %s.",
                                        procedure.identifier(),
                                        ProcedureHint.class.getSimpleName()));
            }
        }
    }

    private Method getMethodFromName(Class<?> clazz, String methodName) {
        Method[] methods = clazz.getDeclaredMethods();

        for (Method method : methods) {
            if (method.getName().equals(methodName)) {
                return method;
            }
        }
        throw new IllegalStateException(
                String.format(
                        "Procedure class %s does not have a method named %s.",
                        clazz.getSimpleName(), methodName));
    }
}
