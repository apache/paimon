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

package org.apache.flink.table.store.utils;

import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

/** Data generator for Flink's internal {@link RowData} type. */
public class RowDataGenerator {

    private final DataGenerator<?>[] fieldGenerators;
    private final String[] fieldNames;

    public RowDataGenerator(DataGenerator<?>[] fieldGenerators, String[] fieldNames)
            throws Exception {
        this.fieldGenerators = fieldGenerators;
        this.fieldNames = fieldNames;
        for (int i = 0; i < fieldGenerators.length; i++) {
            fieldGenerators[i].open(fieldNames[i], null, null);
        }
    }

    public RowData next() {
        GenericRowData row = new GenericRowData(fieldNames.length);
        for (int i = 0; i < fieldGenerators.length; i++) {
            row.setField(i, fieldGenerators[i].next());
        }
        return row;
    }
}
