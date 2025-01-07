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

import java.time.Duration;
import java.util.Map;

/** Factory to create {@link ReplaceTagAction}. */
public class ReplaceTagActionFactory extends CreateOrReplaceTagActionFactory {

    public static final String IDENTIFIER = "replace_tag";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    Action createOrReplaceTagAction(
            String database,
            String table,
            Map<String, String> catalogConfig,
            String tagName,
            Long snapshot,
            Duration timeRetained) {
        return new ReplaceTagAction(
                database, table, catalogConfig, tagName, snapshot, timeRetained);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"replace_tag\" to replace an existing tag with new tag info.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  replace_tag --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> --tag_name <tag_name> [--snapshot <snapshot_id>] [--time_retained <time_retained>]");
        System.out.println();
    }
}
