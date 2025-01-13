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

import org.apache.paimon.utils.StringUtils;

import java.util.Map;

/** Rename Tag action for Flink. */
public class RenameTagAction extends TableActionBase {
    private final String tagName;
    private final String targetTagName;

    public RenameTagAction(
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String tagName,
            String targetTagName) {
        super(databaseName, tableName, catalogConfig);
        this.tagName = tagName;
        this.targetTagName = targetTagName;
    }

    @Override
    public void run() throws Exception {
        if (StringUtils.isEmpty(tagName) || StringUtils.isEmpty(targetTagName)) {
            throw new RuntimeException(
                    String.format(
                            "The specified tag name [%s] or target tag name [%s] cannot be empty.",
                            tagName, targetTagName));
        }
        table.renameTag(tagName, targetTagName);
    }
}
