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

/** Factory to create {@link CompactPostponeBucketAction}. */
public class CompactPostponeBucketActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "compact_postpone_bucket";
    private static final String DEFAULT_BUCKET_NUM = "default_bucket_num";
    private static final String PARALLELISM = "parallelism";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        CompactPostponeBucketAction action =
                new CompactPostponeBucketAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params));

        if (params.has(DEFAULT_BUCKET_NUM)) {
            action.withDefaultBucketNum(Integer.parseInt(params.get(DEFAULT_BUCKET_NUM)));
        }

        if (params.has(PARALLELISM)) {
            action.withParallelism(Integer.parseInt(params.get(PARALLELISM)));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"compact_postpone_bucket\" compacts postpone bucket tables, "
                        + "which distributes records from the temporary \"bucket = -2\" directory "
                        + "into real bucket directories.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  compact_postpone_bucket --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> [--default_bucket_num <default_bucket_num>] "
                        + "[--parallelism <parallelism>]");
        System.out.println(
                "Argument default_bucket_num is the bucket number for the partitions compacted for the first time. "
                        + "The default value is "
                        + CompactPostponeBucketAction.DEFAULT_BUCKET_NUM_NEW_PARTITION
                        + ".");
    }
}
