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

import java.util.Map;
import java.util.Optional;

/** Factory to create {@link CreateTagFromTimestampAction}. */
public class CreateTagFromTimestampActionFactory implements ActionFactory {
    public static final String IDENTIFIER = "create_tag_from_timestamp";

    private static final String TAG = "tag";

    private static final String TIMESTAMP = "timestamp";

    private static final String TIME_RETAINED = "time_retained";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Long timestamp = Long.parseLong(params.get(TIMESTAMP));
        String timeRetained = params.get(TIME_RETAINED);
        Map<String, String> catalogConfig = catalogConfigMap(params);

        CreateTagFromTimestampAction createTagFromTimestampAction =
                new CreateTagFromTimestampAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        params.getRequired(TAG),
                        timestamp,
                        timeRetained,
                        catalogConfig);
        return Optional.of(createTagFromTimestampAction);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"create_tag_from_timestamp\" create tag from timestamp.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  create_tag_from_timestamp --warehouse <warehouse_path> "
                        + "--database <database_name>"
                        + "--table <table_name> "
                        + "--tag <tag> "
                        + "--timestamp <timestamp> "
                        + "[--timeRetained <duration>] "
                        + "[--options <key>=<value>,<key>=<value>,...]");
    }
}
