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

package org.apache.paimon.flink.action.cdc.kafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** IT cases for {@link KafkaSyncTableAction}. */
public class KafkaAWSDMSSyncTableActionITCase extends KafkaSyncTableActionITCase {

    private static final String AWSDMS = "aws-dms";

    @Test
    @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        runSingleTableSchemaEvolution("schemaevolution", AWSDMS);
    }

    @Test
    @Timeout(60)
    public void testAssertSchemaCompatible() throws Exception {
        testAssertSchemaCompatible(AWSDMS);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionSpecific() throws Exception {
        testStarUpOptionSpecific(AWSDMS);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionLatest() throws Exception {
        testStarUpOptionLatest(AWSDMS);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionTimestamp() throws Exception {
        testStarUpOptionTimestamp(AWSDMS);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionEarliest() throws Exception {
        testStarUpOptionEarliest(AWSDMS);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionGroup() throws Exception {
        testStarUpOptionGroup(AWSDMS);
    }

    @Test
    @Timeout(60)
    public void testComputedColumn() throws Exception {
        testComputedColumn(AWSDMS);
    }

    @Test
    @Timeout(60)
    public void testFieldValNullSyncTable() throws Exception {
        testTableFiledValNull(AWSDMS);
    }
}
