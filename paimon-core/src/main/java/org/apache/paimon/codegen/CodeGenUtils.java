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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.guava30.com.google.common.cache.Cache;
import org.apache.paimon.shade.guava30.com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/** Utils for code generations. */
public class CodeGenUtils {

    static final Cache<ClassKey, Pair<Class<?>, Object[]>> COMPILED_CLASS_CACHE =
            CacheBuilder.newBuilder()
                    // assume the table schema will stay the same for a period of time
                    .expireAfterAccess(Duration.ofMinutes(30))
                    // estimated cache size
                    .maximumSize(300)
                    .softValues()
                    .build();

    public static final Projection EMPTY_PROJECTION = input -> BinaryRow.EMPTY_ROW;

    public static Projection newProjection(RowType inputType, List<String> fields) {
        List<String> fieldNames = inputType.getFieldNames();
        int[] mapping = fields.stream().mapToInt(fieldNames::indexOf).toArray();
        return newProjection(inputType, mapping);
    }

    public static Projection newProjection(RowType inputType, int[] mapping) {
        if (mapping.length == 0) {
            return EMPTY_PROJECTION;
        }
        String className = "Projection";
        ClassKey classKey =
                new ClassKey(Projection.class, className, inputType.getFieldTypes(), mapping);

        try {
            Pair<Class<?>, Object[]> classPair =
                    COMPILED_CLASS_CACHE.get(
                            classKey,
                            () -> {
                                GeneratedClass<Projection> generatedClass =
                                        CodeGenLoader.getCodeGenerator()
                                                .generateProjection(className, inputType, mapping);
                                return Pair.of(
                                        generatedClass.compile(CodeGenUtils.class.getClassLoader()),
                                        generatedClass.getReferences());
                            });

            return (Projection)
                    classPair
                            .getLeft()
                            .getConstructor(Object[].class)
                            .newInstance(new Object[] {classPair.getRight()});
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not instantiate generated class '" + className + "'", e);
        }
    }

    public static NormalizedKeyComputer newNormalizedKeyComputer(
            List<DataType> inputTypes, int[] sortFields, String name) {
        ClassKey classKey = new ClassKey(NormalizedKeyComputer.class, name, inputTypes, sortFields);

        try {
            Pair<Class<?>, Object[]> classPair =
                    COMPILED_CLASS_CACHE.get(
                            classKey,
                            () -> {
                                GeneratedClass<NormalizedKeyComputer> generatedClass =
                                        CodeGenLoader.getCodeGenerator()
                                                .generateNormalizedKeyComputer(
                                                        inputTypes, sortFields, name);
                                return Pair.of(
                                        generatedClass.compile(CodeGenUtils.class.getClassLoader()),
                                        generatedClass.getReferences());
                            });

            return (NormalizedKeyComputer)
                    classPair
                            .getLeft()
                            .getConstructor(Object[].class)
                            .newInstance(new Object[] {classPair.getRight()});
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate generated class '" + name + "'", e);
        }
    }

    public static RecordEqualiser newRecordEqualiser(List<DataType> inputTypes, String name) {
        ClassKey classKey =
                new ClassKey(
                        RecordEqualiser.class,
                        name,
                        inputTypes,
                        IntStream.range(0, inputTypes.size()).toArray());

        try {
            Pair<Class<?>, Object[]> classPair =
                    COMPILED_CLASS_CACHE.get(
                            classKey,
                            () -> {
                                GeneratedClass<RecordEqualiser> generatedClass =
                                        generateRecordEqualiser(inputTypes, name);
                                return Pair.of(
                                        generatedClass.compile(CodeGenUtils.class.getClassLoader()),
                                        generatedClass.getReferences());
                            });

            return (RecordEqualiser)
                    classPair
                            .getLeft()
                            .getConstructor(Object[].class)
                            .newInstance(new Object[] {classPair.getRight()});
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate generated class '" + name + "'", e);
        }
    }

    public static RecordComparator newRecordComparator(List<DataType> inputTypes, String name) {
        return newRecordComparator(
                inputTypes, IntStream.range(0, inputTypes.size()).toArray(), name);
    }

    public static RecordComparator newRecordComparator(
            List<DataType> inputTypes, int[] sortFields, String name) {
        ClassKey classKey = new ClassKey(RecordComparator.class, name, inputTypes, sortFields);

        try {
            Pair<Class<?>, Object[]> classPair =
                    COMPILED_CLASS_CACHE.get(
                            classKey,
                            () -> {
                                GeneratedClass<RecordComparator> generatedClass =
                                        generateRecordComparator(inputTypes, sortFields, name);
                                return Pair.of(
                                        generatedClass.compile(CodeGenUtils.class.getClassLoader()),
                                        generatedClass.getReferences());
                            });

            return (RecordComparator)
                    classPair
                            .getLeft()
                            .getConstructor(Object[].class)
                            .newInstance(new Object[] {classPair.getRight()});
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate generated class '" + name + "'", e);
        }
    }

    public static GeneratedClass<RecordComparator> generateRecordComparator(
            List<DataType> inputTypes, int[] sortFields, String name) {
        return CodeGenLoader.getCodeGenerator()
                .generateRecordComparator(inputTypes, sortFields, name);
    }

    public static GeneratedClass<RecordEqualiser> generateRecordEqualiser(
            List<DataType> fieldTypes, String name) {
        return CodeGenLoader.getCodeGenerator().generateRecordEqualiser(fieldTypes, name);
    }

    /** Class to use as key for the {@link #COMPILED_CLASS_CACHE}. */
    public static class ClassKey {

        private final Class<?> classType;

        private final String className;

        private final List<DataType> fields;

        private final int[] fieldsIndex;

        public ClassKey(
                Class<?> classType, String className, List<DataType> fields, int[] fieldsIndex) {
            this.classType = classType;
            this.className = className;
            this.fields = fields;
            this.fieldsIndex = fieldsIndex;
        }

        public Class<?> getClassType() {
            return classType;
        }

        public String getClassName() {
            return className;
        }

        public List<DataType> getFields() {
            return fields;
        }

        public int[] getFieldsIndex() {
            return fieldsIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClassKey classKey = (ClassKey) o;
            return Objects.equals(classType, classKey.classType)
                    && Objects.equals(className, classKey.className)
                    && Objects.equals(fields, classKey.fields)
                    && Arrays.equals(fieldsIndex, classKey.fieldsIndex);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(classType, className, fields);
            result = 31 * result + Arrays.hashCode(fieldsIndex);
            return result;
        }
    }
}
