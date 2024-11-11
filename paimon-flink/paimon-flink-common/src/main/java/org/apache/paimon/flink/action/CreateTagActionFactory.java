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

import org.apache.flink.api.java.tuple.Tuple3;

import java.time.Duration;
import java.util.Map;

/** Factory to create {@link CreateTagAction}. */
public class CreateTagActionFactory extends CreateOrReplaceTagActionFactory {

    public static final String IDENTIFIER = "create_tag";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    Action createOrReplaceTagAction(
            Tuple3<String, String, String> tablePath,
            Map<String, String> catalogConfig,
            String tagName,
            Long snapshot,
            Duration timeRetained) {
        return new CreateTagAction(
                tablePath.f0,
                tablePath.f1,
                tablePath.f2,
                catalogConfig,
                tagName,
                snapshot,
                timeRetained);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"create_tag\" creates a tag from given snapshot.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  create_tag --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> --tag_name <tag_name> [--snapshot <snapshot_id>]");
        System.out.println();
    }
}
