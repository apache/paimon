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

package org.apache.paimon.flink.iceberg;

/** IT cases for Paimon Iceberg compatibility in Flink 1.17. */
public class Flink117IcebergITCase extends FlinkIcebergITCaseBase {
    @Override
    public void testCreateTags(String format) throws Exception {
        // Flink 1.17 does not support create_tag procedure so we skip this test.
    }

    @Override
    public void testDeleteTags(String format) throws Exception {
        // Flink 1.17 does not support delete_tag procedure so we skip this test.
    }

    @Override
    public void testReplaceTags(String format) throws Exception {
        // Flink 1.17 does not support replace_tag procedure so we skip this test.
    }
}
