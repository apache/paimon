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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.table.Table;

import java.time.Duration;

/** A procedure to replace a tag. */
public class ReplaceTagProcedure extends CreateOrReplaceTagBaseProcedure {

    private static final String IDENTIFIER = "replace_tag";

    @Override
    void createOrReplaceTag(Table table, String tagName, Long snapshotId, Duration timeRetained) {
        if (snapshotId == null) {
            table.replaceTag(tagName, timeRetained);
        } else {
            table.replaceTag(tagName, snapshotId, timeRetained);
        }
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
