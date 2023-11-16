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

package org.apache.paimon.table.system;

import org.apache.paimon.lineage.LineageMeta;
import org.apache.paimon.lineage.LineageMetaFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;

import java.util.Map;

/**
 * This is a system table to display all the source table lineages.
 *
 * <pre>
 *  For example:
 *     If we select * from sys.source_table_lineage, we will get
 *     database_name       table_name       job_name      create_time
 *        default            test0            job1    2023-10-22 20:35:12
 *       database1           test1            job1    2023-10-28 21:35:52
 *          ...               ...             ...             ...
 *     We can write sql to fetch the information we need.
 * </pre>
 */
public class SourceTableLineageTable extends TableLineageTable {

    public static final String SOURCE_TABLE_LINEAGE = "source_table_lineage";

    public SourceTableLineageTable(LineageMetaFactory lineageMetaFactory, Options options) {
        super(lineageMetaFactory, options);
    }

    @Override
    public InnerTableRead newRead() {
        return new TableLineageRead(lineageMetaFactory, options, LineageMeta::sourceTableLineages);
    }

    @Override
    public String name() {
        return SOURCE_TABLE_LINEAGE;
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new SourceTableLineageTable(lineageMetaFactory, options);
    }
}
