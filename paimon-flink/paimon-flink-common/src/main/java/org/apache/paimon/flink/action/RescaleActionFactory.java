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

package org.apache.paimon.flink.action;

import java.util.Optional;

/** Factory to create {@link RescaleAction}. */
public class RescaleActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "rescale";
    private static final String BUCKET_NUM = "bucket_num";
    private static final String PARTITION = "partition";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        RescaleAction action =
                new RescaleAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params));

        if (params.has(BUCKET_NUM)) {
            action.withBucketNum(Integer.parseInt(params.get(BUCKET_NUM)));
        }

        if (params.has(PARTITION)) {
            action.withPartition(getPartitions(params).get(0));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"rescale\" rescales one partition of a table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  rescale --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> [--bucket_num <bucket_num>] "
                        + "[--partition <partition>]");
        System.out.println(
                "The default value of argument bucket_num is the current bucket number of the table. "
                        + "For postpone bucket tables, this argument must be specified.");
        System.out.println(
                "Argument partition must be specified if the table is a partitioned table.");
    }
}
