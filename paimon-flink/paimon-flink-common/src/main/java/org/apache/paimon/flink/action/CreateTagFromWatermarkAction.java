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

import org.apache.paimon.flink.procedure.CreateTagFromWatermarkProcedure;

import java.util.Map;

/** Create tag from watermark action for Flink. */
public class CreateTagFromWatermarkAction extends ActionBase implements LocalAction {

    private final String database;
    private final String table;
    private final String tag;
    private final Long watermark;
    private final String timeRetained;

    public CreateTagFromWatermarkAction(
            String database,
            String table,
            String tag,
            Long watermark,
            String timeRetained,
            Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.database = database;
        this.table = table;
        this.tag = tag;
        this.watermark = watermark;
        this.timeRetained = timeRetained;
    }

    @Override
    public void executeLocally() throws Exception {
        CreateTagFromWatermarkProcedure createTagFromWatermarkProcedure =
                new CreateTagFromWatermarkProcedure();
        createTagFromWatermarkProcedure.withCatalog(catalog);
        createTagFromWatermarkProcedure.call(
                null, database + "." + table, tag, watermark, timeRetained);
    }
}
