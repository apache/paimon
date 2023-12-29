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

package org.apache.paimon.flink;

import org.apache.paimon.flink.action.DeleteAction;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for Flink action. */
public class FlinkActionITCase extends CatalogITCaseBase {

    protected int defaultParallelism() {
        return 1;
    }

    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE T (k INT, v STRING, PRIMARY KEY (k) NOT ENFORCED)");
    }

    @Test
    public void testDeleteAction() throws Exception {
        batchSql("INSERT INTO T VALUES (1, 'Hi'), (2, 'Hello'), (3, 'World')");

        DeleteAction action =
                new DeleteAction(path, "default", "T", "k = 1", Collections.emptyMap());

        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(sEnv.executeSql("SELECT * FROM T").collect());

        action.run();

        assertThat(iterator.collect(4))
                .containsExactlyInAnyOrder(
                        changelogRow("+I", 1, "Hi"),
                        changelogRow("+I", 2, "Hello"),
                        changelogRow("+I", 3, "World"),
                        changelogRow("-D", 1, "Hi"));
        iterator.close();
    }
}
