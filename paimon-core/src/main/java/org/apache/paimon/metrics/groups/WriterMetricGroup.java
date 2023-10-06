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

package org.apache.paimon.metrics.groups;

import org.apache.paimon.metrics.AbstractMetricGroup;

import java.util.HashMap;
import java.util.Map;

/** A Writer MetricGroup {@link org.apache.paimon.metrics.MetricGroup}. */
public class WriterMetricGroup extends AbstractMetricGroup {

    private final String groupName;

    WriterMetricGroup(final Map<String, String> tags, final String groupName) {
        super(tags);
        this.groupName = groupName;
    }

    public static WriterMetricGroup createWriterMetricGroup(
            final String table, final String groupName, final Map<String, String> externTags) {
        Map<String, String> tags = new HashMap<>(externTags);
        tags.put("table", table);
        return new WriterMetricGroup(tags, groupName);
    }

    @Override
    public String getGroupName() {
        return groupName;
    }
}
