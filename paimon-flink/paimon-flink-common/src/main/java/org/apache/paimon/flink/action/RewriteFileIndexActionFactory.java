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

/** Factory to create {@link RewriteFileIndexAction}. */
public class RewriteFileIndexActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "rewrite_file_index";

    private static final String IDENTIFIER_KEY = "identifier";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Map<String, String> catalogConfig = catalogConfigMap(params);
        String identifier = params.get(IDENTIFIER_KEY);

        RewriteFileIndexAction action = new RewriteFileIndexAction(identifier, catalogConfig);

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"rewrite_file_index\" Rewrite the file index for the table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  rewrite_file_index --warehouse <warehouse_path> --identifier <database.table> ");
    }
}
