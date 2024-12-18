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

/** Factory to create {@link ExpireSnapshotsAction}. */
public class ExpireSnapshotsActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "expire_snapshots";

    private static final String IDENTIFIER_KEY = "identifier";

    private static final String RETAIN_MAX = "retain_max";
    private static final String RETAIN_MIN = "retain_min";
    private static final String OLDER_THAN = "older_than";
    private static final String MAX_DELETES = "max_deletes";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Integer retainMax =
                params.has(RETAIN_MAX) ? Integer.parseInt(params.get(RETAIN_MAX)) : null;
        Integer retainMin =
                params.has(RETAIN_MIN) ? Integer.parseInt(params.get(RETAIN_MIN)) : null;
        String olderThan = params.has(OLDER_THAN) ? params.get(OLDER_THAN) : null;
        Integer maxDeletes =
                params.has(MAX_DELETES) ? Integer.parseInt(params.get(MAX_DELETES)) : null;

        ExpireSnapshotsAction action =
                new ExpireSnapshotsAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params),
                        retainMax,
                        retainMin,
                        olderThan,
                        maxDeletes);

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"expire_snapshots\" expire the target snapshots.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  expire_snapshots --warehouse <warehouse_path> --database <database> --table <table> --retain_max <max> --retain_min <min> --older_than <older_than> --max_delete <max_delete>");
    }
}
