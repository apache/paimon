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
 * This is a system table to display all the sink data lineages.
 *
 * <pre>
 *  For example:
 *     If we select * from sys.sink_data_lineage, we will get
 *  database_name  table_name  barrier_id  snapshot_id   job_name   create_time
 *    default      test0        1L           2L          job1   2023-10-22 20:35:12
 *   database      test1        1L           3L          job1   2023-10-28 21:35:52
 *     ...         ...         ...          ...          ...        ...
 *     We can write sql to fetch the information we need.
 * </pre>
 */
public class SinkDataLineageTable extends DataLineageTable {

    public static final String SINK_DATA_LINEAGE = "sink_data_lineage";

    protected SinkDataLineageTable(LineageMetaFactory lineageMetaFactory, Options options) {
        super(lineageMetaFactory, options);
    }

    @Override
    public InnerTableRead newRead() {
        return new DataLineageTable.DataLineageRead(
                lineageMetaFactory, options, LineageMeta::sinkDataLineages);
    }

    @Override
    public String name() {
        return SINK_DATA_LINEAGE;
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new SinkDataLineageTable(lineageMetaFactory, options);
    }
}
