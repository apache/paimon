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

package org.apache.paimon.flink.action;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Test to verify that all Flink Actions support starting Flink jobs. */
public class ActionJobCoverageTest {
    private static final Logger LOG = LoggerFactory.getLogger(ActionJobCoverageTest.class);

    @Test
    public void testAllActionSupportsFlinkJob() throws Exception {
        // Get all Action subclasses using reflections
        Reflections reflections =
                new Reflections(
                        new ConfigurationBuilder()
                                .setUrls(ClasspathHelper.forPackage("org.apache.paimon.flink"))
                                .setScanners(Scanners.SubTypes));

        Set<Class<? extends Action>> actionClasses =
                reflections.getSubTypesOf(Action.class).stream()
                        .filter(x -> !Modifier.isAbstract(x.getModifiers()) && !x.isInterface())
                        .collect(Collectors.toSet());

        assertThat(actionClasses).isNotEmpty();

        // In order for an action to support flink job, it should satisfy any of the following
        // requirements.
        for (Class<? extends Action> actionClass : actionClasses) {
            // 1. it implements LocalAction.
            if (LocalAction.class.isAssignableFrom(actionClass)) {
                continue;
            }

            Method runMethod = actionClass.getMethod("run");

            // 2. Its run() method body has env.execute() calls.
            if (haveEnvExecutePattern(runMethod)) {
                continue;
            }

            fail(String.format("Action %s might not support starting Flink jobs.", actionClass));
        }
    }

    private boolean haveEnvExecutePattern(Method runMethod) throws IOException {
        // Use ASM to analyze the bytecode of the run() method
        String className = runMethod.getDeclaringClass().getName().replace('.', '/');
        String methodName = runMethod.getName();
        String methodDesc = org.objectweb.asm.Type.getMethodDescriptor(runMethod);

        InputStream classFile =
                runMethod.getDeclaringClass().getResourceAsStream("/" + className + ".class");
        if (classFile == null) {
            LOG.warn("Failed to load class file for {}", className);
            return false;
        }

        ClassReader classReader = new ClassReader(classFile);
        EnvExecuteMethodVisitor visitor = new EnvExecuteMethodVisitor(methodName, methodDesc);
        classReader.accept(visitor, 0);

        return visitor.hasEnvExecuteCall();
    }

    /** ASM visitor to check if a method contains env.execute() calls. */
    private static class EnvExecuteMethodVisitor extends ClassVisitor {
        private final String targetMethodName;
        private final String targetMethodDesc;
        private boolean hasEnvExecuteCall = false;

        private static final List<Tuple2<String, String>> VALID_OWNER_AND_NAMES =
                Arrays.asList(
                        Tuple2.of(
                                "org/apache/flink/streaming/api/environment/StreamExecutionEnvironment",
                                "execute"),
                        Tuple2.of("org/apache/flink/table/api/TableResult", "await"),
                        Tuple2.of(
                                "org/apache/flink/table/procedure/DefaultProcedureContext",
                                "<init>"),
                        Tuple2.of(
                                "org/apache/paimon/flink/orphan/FlinkOrphanFilesClean",
                                "executeDatabaseOrphanFiles"));

        private static final List<Tuple2<String, String>> VALID_OWNER_PATTERN_AND_NAMES =
                Collections.singletonList(
                        Tuple2.of(
                                "org/apache/paimon/flink/action/.*".replace("/", "\\/"),
                                "execute"));

        public EnvExecuteMethodVisitor(String methodName, String methodDesc) {
            super(Opcodes.ASM9);
            this.targetMethodName = methodName;
            this.targetMethodDesc = methodDesc;
        }

        @Override
        public MethodVisitor visitMethod(
                int access, String name, String descriptor, String signature, String[] exceptions) {
            if (name.equals(targetMethodName) && descriptor.equals(targetMethodDesc)) {
                return new MethodVisitor(Opcodes.ASM9) {
                    @Override
                    public void visitMethodInsn(
                            int opcode,
                            String owner,
                            String name,
                            String descriptor,
                            boolean isInterface) {
                        // Check for env.execute() calls
                        if (!hasEnvExecuteCall) {
                            for (Tuple2<String, String> validOwnerAndName : VALID_OWNER_AND_NAMES) {
                                if (owner.equals(validOwnerAndName.f0)
                                        && name.equals(validOwnerAndName.f1)) {
                                    hasEnvExecuteCall = true;
                                    break;
                                }
                            }

                            for (Tuple2<String, String> validOwnerPatternAndName :
                                    VALID_OWNER_PATTERN_AND_NAMES) {
                                if (owner.matches(validOwnerPatternAndName.f0)
                                        && name.equals(validOwnerPatternAndName.f1)) {
                                    hasEnvExecuteCall = true;
                                    break;
                                }
                            }
                        }

                        super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
                    }
                };
            }
            return super.visitMethod(access, name, descriptor, signature, exceptions);
        }

        public boolean hasEnvExecuteCall() {
            return hasEnvExecuteCall;
        }
    }
}
