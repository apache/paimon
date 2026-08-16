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

import org.apache.paimon.catalog.Identifier;

import java.util.Optional;

/** Factory to create {@link MergeBranchAction}. */
public class MergeBranchActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "merge_branch";

    private static final String SOURCE_BRANCH = "source_branch";
    private static final String TARGET_BRANCH = "target_branch";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        String targetBranch =
                params.has(TARGET_BRANCH)
                        ? params.getRequired(TARGET_BRANCH)
                        : Identifier.DEFAULT_MAIN_BRANCH;
        MergeBranchAction action =
                new MergeBranchAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params),
                        params.getRequired(SOURCE_BRANCH),
                        targetBranch);
        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"merge_branch\" merges data files from a source branch into a target branch.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  merge_branch \\\n"
                        + "--warehouse <warehouse_path> \\\n"
                        + "--database <database_name> \\\n"
                        + "--table <table_name> \\\n"
                        + "--source_branch <source_branch_name> \\\n"
                        + "[--target_branch <target_branch_name>]");
        System.out.println();
    }
}
