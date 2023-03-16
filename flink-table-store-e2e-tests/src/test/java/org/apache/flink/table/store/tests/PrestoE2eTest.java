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

package org.apache.flink.table.store.tests;

import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/** Tests for reading table store from Presto-0.273. */
@DisabledIfSystemProperty(named = "test.flink.version", matches = "1.14.*")
public class PrestoE2eTest extends E2eReaderTestBase {
    // TODO
    public PrestoE2eTest(boolean withKafka, boolean withHive, boolean withSpark) {
        super(withKafka, withHive, withSpark);
    }
}
