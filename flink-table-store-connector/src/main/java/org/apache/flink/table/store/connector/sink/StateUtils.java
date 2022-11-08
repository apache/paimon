/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Utility class for sink state manipulation. */
public class StateUtils {

    public static @Nullable <T> T getSingleValueFromState(
            StateInitializationContext context,
            String stateName,
            Class<T> valueClass,
            T defaultValue)
            throws Exception {
        ListState<T> state =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>(stateName, valueClass));

        List<T> values = new ArrayList<>();
        state.get().forEach(values::add);

        if (context.isRestored()) {
            // Values may contain 0 element or more than 1 element.
            //
            // Let's say a vertex has 3 tasks (A, B and C). If A is finished while B and C are still
            // running, then states of A will be divided between B and C. That is, if the job
            // restarts, state of vertex A will be empty, and state of vertex B and C may contain
            // more than 1 element.
            if (values.isEmpty()) {
                return null;
            }

            // As we're storing the same value for each task, we hereby check if all elements are
            // equal.
            for (int i = 1; i < values.size(); i++) {
                Preconditions.checkState(
                        values.get(i).equals(values.get(i - 1)),
                        "Values in list state are not the same. This is unexpected.");
            }
        } else {
            Preconditions.checkState(
                    values.isEmpty(),
                    "Expecting 0 value for a fresh state but found "
                            + values.size()
                            + ". This is unexpected.");
            state.add(defaultValue);
            values.add(defaultValue);
        }

        return values.get(0);
    }
}
