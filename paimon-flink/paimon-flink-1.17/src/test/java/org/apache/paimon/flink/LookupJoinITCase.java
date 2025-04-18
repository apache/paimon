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

import org.apache.paimon.flink.FlinkConnectorOptions.LookupCacheMode;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for lookup join for Flink 1.17. */
public class LookupJoinITCase extends CatalogITCaseBase {

    @Override
    public List<String> ddl() {
        return Collections.singletonList("CREATE TABLE T (i INT, `proctime` AS PROCTIME())");
    }

    @Override
    protected int defaultParallelism() {
        return 1;
    }

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testLookupMaxPtPartitionedTable(LookupCacheMode mode) throws Exception {
        boolean testDynamicBucket = ThreadLocalRandom.current().nextBoolean();
        String primaryKeys;
        String bucket;
        if (testDynamicBucket) {
            primaryKeys = "k";
            bucket = "-1";
        } else {
            primaryKeys = "pt, k";
            bucket = "1";
        }
        sql(
                "CREATE TABLE PARTITIONED_DIM (pt STRING, k INT, v INT, PRIMARY KEY (%s) NOT ENFORCED)"
                        + "PARTITIONED BY (`pt`) WITH ("
                        + "'bucket' = '%s', "
                        + "'lookup.dynamic-partition' = 'max_pt()', "
                        + "'lookup.dynamic-partition.refresh-interval' = '1 ms', "
                        + "'lookup.cache' = '%s', "
                        + "'continuous.discovery-interval'='1 ms')",
                primaryKeys, bucket, mode);
        String query =
                "SELECT T.i, D.v FROM T LEFT JOIN PARTITIONED_DIM for system_time as of T.proctime AS D ON T.i = D.k";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO PARTITIONED_DIM VALUES ('1', 1, 2)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1)");
        List<Row> result = iterator.collect(1);
        assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2));

        sql("INSERT INTO PARTITIONED_DIM VALUES ('2', 1, 3)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1)");
        result = iterator.collect(1);
        assertThat(result).containsExactlyInAnyOrder(Row.of(1, 3));

        iterator.close();
    }
}
