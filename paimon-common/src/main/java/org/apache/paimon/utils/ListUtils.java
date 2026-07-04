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

package org.apache.paimon.utils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for {@link List}. */
public class ListUtils {

    public static <T> T pickRandomly(List<T> list) {
        checkArgument(!list.isEmpty(), "list is empty");
        int index = ThreadLocalRandom.current().nextInt(list.size());
        return list.get(index);
    }

    public static <T> boolean isNullOrEmpty(Collection<T> list) {
        return list == null || list.isEmpty();
    }

    public static <T> List<T> toList(Iterator<T> iterator) {
        List<T> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    public static <E> List<E> union(List<? extends E> list1, List<? extends E> list2) {
        ArrayList<E> result = new ArrayList<>(list1.size() + list2.size());
        result.addAll(list1);
        result.addAll(list2);
        return result;
    }

    /**
     * Replace the first continuous occurrence of {@code replaced} in {@code list} with {@code
     * replacement}. Return null if {@code replaced} does not appear as a continuous sub-list.
     */
    @Nullable
    public static <E> List<E> tryReplace(
            List<? extends E> list, List<?> replaced, List<? extends E> replacement) {
        checkArgument(!replaced.isEmpty(), "Cannot replace an empty list.");

        for (int start = 0; start <= list.size() - replaced.size(); start++) {
            boolean found = true;
            for (int i = 0; i < replaced.size(); i++) {
                if (!Objects.equals(list.get(start + i), replaced.get(i))) {
                    found = false;
                    break;
                }
            }

            if (found) {
                ArrayList<E> result =
                        new ArrayList<>(list.size() - replaced.size() + replacement.size());
                result.addAll(list.subList(0, start));
                result.addAll(replacement);
                result.addAll(list.subList(start + replaced.size(), list.size()));
                return result;
            }
        }

        return null;
    }
}
