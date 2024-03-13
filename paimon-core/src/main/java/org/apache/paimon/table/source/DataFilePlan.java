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

package org.apache.paimon.table.source;

import org.apache.paimon.table.source.snapshot.StartingScanner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Scanning plan containing snapshot ID and input splits. */
public class DataFilePlan implements TableScan.Plan {

    private final List<DataSplit> splits;

    public DataFilePlan(List<DataSplit> splits) {
        this.splits = splits;
    }

    @Override
    public List<Split> splits() {
        return new ArrayList<>(splits);
    }

    public static TableScan.Plan fromResult(StartingScanner.Result result) {
        return result instanceof StartingScanner.ScannedResult
                ? ((StartingScanner.ScannedResult) result).plan()
                : new DataFilePlan(Collections.emptyList());
    }
}
