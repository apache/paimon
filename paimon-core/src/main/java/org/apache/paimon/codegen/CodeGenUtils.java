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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.apache.paimon.codegen.CodeGenLoader.getCodeGenerator;

/** Utils for code generations. */
public class CodeGenUtils {

    private static final Cache<ClassKey, Pair<Class<?>, Object[]>> COMPILED_CLASS_CACHE =
            CacheBuilder.newBuilder().maximumSize(300).softValues().build();

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

        return generate(
                Projection.class,
                inputType.getFieldTypes(),
                mapping,
                () -> getCodeGenerator().generateProjection(inputType, mapping));
    }

    public static NormalizedKeyComputer newNormalizedKeyComputer(
            List<DataType> inputTypes, int[] sortFields) {
        return generate(
                NormalizedKeyComputer.class,
                inputTypes,
                sortFields,
                () -> getCodeGenerator().generateNormalizedKeyComputer(inputTypes, sortFields));
    }

    public static RecordComparator newRecordComparator(List<DataType> inputTypes) {
        return newRecordComparator(
                inputTypes, IntStream.range(0, inputTypes.size()).toArray(), true);
    }

    public static RecordComparator newRecordComparator(
            List<DataType> inputTypes, int[] sortFields, boolean isAscendingOrder) {
        return generate(
                RecordComparator.class,
                inputTypes,
                sortFields,
                () ->
                        getCodeGenerator()
                                .generateRecordComparator(
                                        inputTypes, sortFields, isAscendingOrder));
    }

    public static RecordEqualiser newRecordEqualiser(List<DataType> fieldTypes) {
        return newRecordEqualiser(fieldTypes, IntStream.range(0, fieldTypes.size()).toArray());
    }

    public static RecordEqualiser newRecordEqualiser(List<DataType> fieldTypes, int[] fields) {
        return generate(
                RecordEqualiser.class,
                fieldTypes,
                fields,
                () -> getCodeGenerator().generateRecordEqualiser(fieldTypes, fields));
    }

    private static <T> T generate(
            Class<?> classType,
            List<DataType> fields,
            int[] fieldsIndex,
            Supplier<GeneratedClass<T>> supplier) {
        ClassKey classKey = new ClassKey(classType, fields, fieldsIndex);

        try {
            Pair<Class<?>, Object[]> result =
                    COMPILED_CLASS_CACHE.get(classKey, () -> generateClass(supplier));

            //noinspection unchecked
            return (T) GeneratedClass.newInstance(result.getLeft(), result.getRight());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not instantiate generated class '" + classType + "'", e);
        }
    }

    private static <T> Pair<Class<?>, Object[]> generateClass(
            Supplier<GeneratedClass<T>> supplier) {
        long time = System.currentTimeMillis();
        RuntimeException ex;

        do {
            try {
                GeneratedClass<T> generatedClass = supplier.get();
                return Pair.of(
                        generatedClass.compile(CodeGenUtils.class.getClassLoader()),
                        generatedClass.getReferences());
            } catch (OutOfMemoryError error) {
                // try to gc meta space
                System.gc();
                try {
                    Thread.sleep(5_000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    throw new RuntimeException("Sleep interrupted", error);
                }
                ex = new RuntimeException("Meet meta space oom while generating class.", error);
            }
        } while ((System.currentTimeMillis() - time) < 60_000);
        throw ex;
    }

    private static class ClassKey {

        private final Class<?> classType;

        private final List<DataType> fields;

        private final int[] fieldsIndex;

        public ClassKey(Class<?> classType, List<DataType> fields, int[] fieldsIndex) {
            this.classType = classType;
            this.fields = fields;
            this.fieldsIndex = fieldsIndex;
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
                    && Objects.equals(fields, classKey.fields)
                    && Arrays.equals(fieldsIndex, classKey.fieldsIndex);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(classType, fields);
            result = 31 * result + Arrays.hashCode(fieldsIndex);
            return result;
        }
    }
}
