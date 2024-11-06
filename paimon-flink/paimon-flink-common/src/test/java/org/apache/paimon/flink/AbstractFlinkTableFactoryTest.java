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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AbstractFlinkTableFactory}. */
public class AbstractFlinkTableFactoryTest {

    @Test
    public void testSchemaEquals() {
        innerTest(RowType.of(false), RowType.of(true), true);
        innerTest(RowType.of(false), RowType.of(false, new VarCharType()), false);
        innerTest(
                RowType.of(new LogicalType[] {new VarCharType()}, new String[] {"foo"}),
                RowType.of(new VarCharType()),
                false);
        innerTest(
                new RowType(
                        true,
                        Arrays.asList(
                                new RowType.RowField("foo", new VarCharType(), "comment about foo"),
                                new RowType.RowField("bar", new IntType()))),
                new RowType(
                        false,
                        Arrays.asList(
                                new RowType.RowField("foo", new VarCharType()),
                                new RowType.RowField("bar", new IntType(), "comment about bar"))),
                true);
    }

    @Test
    public void testGetDynamicOptions() {
        Configuration configuration = new Configuration();
        configuration.setString("k1", "v2");
        ObjectIdentifier identifier = ObjectIdentifier.of("catalog1", "db", "T");
        DynamicTableFactory.Context context =
                new FactoryUtil.DefaultDynamicTableContext(
                        identifier,
                        null,
                        new HashMap<>(),
                        configuration,
                        AbstractFlinkTableFactoryTest.class.getClassLoader(),
                        false);
        Map<String, String> options = AbstractFlinkTableFactory.getDynamicConfigOptions(context);
        assertThat(options).isEqualTo(ImmutableMap.of("k1", "v2"));

        configuration = new Configuration();
        configuration.setString("k1", "v2");
        configuration.setString("k3", "v3");
        configuration.setString("paimon.catalog1.db.T.k1", "v1");
        configuration.setString("paimon.*.db.*.k2", "v2");
        identifier = ObjectIdentifier.of("catalog1", "db", "T");
        context =
                new FactoryUtil.DefaultDynamicTableContext(
                        identifier,
                        null,
                        new HashMap<>(),
                        configuration,
                        AbstractFlinkTableFactoryTest.class.getClassLoader(),
                        false);
        options = AbstractFlinkTableFactory.getDynamicConfigOptions(context);
        assertThat(options).isEqualTo(ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"));
    }

    private void innerTest(RowType r1, RowType r2, boolean expectEquals) {
        assertThat(AbstractFlinkTableFactory.schemaEquals(r1, r2)).isEqualTo(expectEquals);
    }
}
