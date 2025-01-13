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

/** Factory to create {@link RenameTagActionFactory}. */
public class RenameTagActionFactory implements ActionFactory {

    private static final String IDENTIFIER = "rename_tag";

    private static final String TAG_NAME = "tag_name";
    private static final String TARGET_TAG_NAME = "target_tag_name";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        RenameTagAction action =
                new RenameTagAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params),
                        params.getRequired(TAG_NAME),
                        params.getRequired(TARGET_TAG_NAME));
        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"rename_tag\" rename a tag with a new tag name.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  rename_tag --warehouse <warehouse_path> --tag_name <tag_name> "
                        + "--target_tag_name <target_tag_name>");
        System.out.println();
    }
}
