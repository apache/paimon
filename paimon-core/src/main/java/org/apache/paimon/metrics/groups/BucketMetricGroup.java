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

/** Special {@link org.apache.paimon.metrics.MetricGroup} representing a bucket. */
public class BucketMetricGroup extends AbstractMetricGroup {
    private final String groupName;

    // ------------------------------------------------------------------------

    BucketMetricGroup(final Map<String, String> tags, final String groupName) {
        super(tags);
        this.groupName = groupName;
    }

    public static BucketMetricGroup createBucketMetricGroup(
            final String table, final int bucket, final String partition, final String groupName) {
        Map<String, String> tags = new HashMap<>();
        tags.put("table", table);
        tags.put("bucket", String.valueOf(bucket));
        tags.put("partition", partition);
        return new BucketMetricGroup(tags, groupName);
    }

    @Override
    public String getGroupName() {
        return this.groupName;
    }
}
