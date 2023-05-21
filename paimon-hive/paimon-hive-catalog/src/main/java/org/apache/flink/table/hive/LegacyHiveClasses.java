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

package org.apache.flink.table.hive;

import org.apache.hadoop.hive.metastore.api.Table;

/** Legacy hive classes of table store 0.3. */
@Deprecated
public class LegacyHiveClasses {

    private static final String LEGACY_INPUT_FORMAT_CLASS_NAME =
            "org.apache.flink.table.store.mapred.TableStoreInputFormat";
    private static final String LEGACY_OUTPUT_FORMAT_CLASS_NAME =
            "org.apache.flink.table.store.mapred.TableStoreOutputFormat";

    public static boolean isPaimonTable(Table table) {
        return LEGACY_INPUT_FORMAT_CLASS_NAME.equals(table.getSd().getInputFormat())
                && LEGACY_OUTPUT_FORMAT_CLASS_NAME.equals(table.getSd().getOutputFormat());
    }
}
