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

package org.apache.paimon.table;

import org.apache.paimon.utils.CompactMetricsManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** An implementation for {@link ExpireCompactMetrics}. */
public class ExpireCompactMetricsImpl implements ExpireCompactMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(ExpireCompactMetricsImpl.class);

    private final CompactMetricsManager compactMetricsManager;
    private final long retainNum;

    public ExpireCompactMetricsImpl(CompactMetricsManager compactMetricsManager, long retainNum) {
        this.compactMetricsManager = compactMetricsManager;
        this.retainNum = retainNum;
    }

    @Override
    public int expire() {
        try {
            int expireNum = 0;
            if (compactMetricsManager.compactMetricCount() > retainNum) {
                List<Long> metrics = compactMetricsManager.compactMetricIds();
                for (int i = 0; i < metrics.size() - retainNum; i++) {
                    compactMetricsManager.deleteCompactMetric(metrics.get(i));
                }
                expireNum = (int) (metrics.size() - retainNum);
            }
            LOG.debug("Expire compact metrics size {}", expireNum);
            return expireNum;
        } catch (Exception e) {
            LOG.error("Expire compact metrics error", e);
            return -1;
        }
    }
}
