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

/** Factory to create {@link ExpireTagsAction}. */
public class ExpireTagsActionFactory implements ActionFactory {

    private static final String IDENTIFIER = "expire_tags";

    private static final String OLDER_THAN = "older_than";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        ExpireTagsAction expireTagsAction =
                new ExpireTagsAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        params.get(OLDER_THAN),
                        catalogConfigMap(params));
        return Optional.of(expireTagsAction);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"expire_tags\" expire tags by time.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  expire_tags --warehouse <warehouse_path> "
                        + "--database <database>"
                        + "--table <table> "
                        + "[--older_than <older_than>]");
    }
}
