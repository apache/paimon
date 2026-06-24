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

package org.apache.paimon.fileindex;

import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileIndexPredicate}. */
public class FileIndexPredicateTest {

    @Test
    public void testGetRequiredNamesVisitsEachChildOnce() throws Exception {
        CountingLeafPredicate left = new CountingLeafPredicate(0, "a");
        CountingLeafPredicate right = new CountingLeafPredicate(1, "b");
        Predicate predicate = new CompoundPredicate(Or.INSTANCE, Arrays.asList(left, right));

        Set<String> requiredNames = getRequiredNames(predicate);

        assertThat(requiredNames).containsExactlyInAnyOrder("a", "b");
        assertThat(left.visitCount).isEqualTo(1);
        assertThat(right.visitCount).isEqualTo(1);
    }

    @SuppressWarnings("unchecked")
    private static Set<String> getRequiredNames(Predicate predicate) throws Exception {
        Method method =
                FileIndexPredicate.class.getDeclaredMethod("getRequiredNames", Predicate.class);
        method.setAccessible(true);
        return (Set<String>) method.invoke(emptyFileIndexPredicate(), predicate);
    }

    private static FileIndexPredicate emptyFileIndexPredicate() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos);
        writer.writeColumnIndexes(new HashMap<String, java.util.Map<String, byte[]>>());
        writer.close();
        return new FileIndexPredicate(baos.toByteArray(), RowType.builder().build());
    }

    private static class CountingLeafPredicate extends LeafPredicate {

        private int visitCount;

        private CountingLeafPredicate(int fieldIndex, String fieldName) {
            super(
                    Equal.INSTANCE,
                    DataTypes.INT(),
                    fieldIndex,
                    fieldName,
                    Collections.singletonList(1));
        }

        @Override
        public <T> T visit(PredicateVisitor<T> visitor) {
            visitCount++;
            return super.visit(visitor);
        }
    }
}
