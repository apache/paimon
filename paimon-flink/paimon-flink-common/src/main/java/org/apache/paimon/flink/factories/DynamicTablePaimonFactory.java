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

package org.apache.paimon.flink.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.factories.FormatFactory;

import java.util.Collections;
import java.util.Set;

/**
 * Base interface for configuring a dynamic table connector for an external Log Systems from catalog
 * and session information.
 *
 * <p>Dynamic tables are the core concept of Flink's Table & SQL API for processing both bounded and
 * unbounded data in a unified fashion.
 */
public interface DynamicTablePaimonFactory extends PaimonFactory {

    /**
     * Returns a set of {@link ConfigOption} that are directly forwarded to the runtime
     * implementation but don't affect the final execution topology.
     *
     * <p>Options declared here can override options of the persisted plan during an enrichment
     * phase. Since a restored topology is static, an implementer has to ensure that the declared
     * options don't affect fundamental abilities such as {@link SupportsProjectionPushDown} or
     * {@link SupportsFilterPushDown}.
     *
     * <p>For example, given a database connector, if an option defines the connection timeout,
     * changing this value does not affect the pipeline topology and can be allowed. However, an
     * option that defines whether the connector supports {@link SupportsReadingMetadata} or not is
     * not allowed. The planner might not react to changed abilities anymore.
     *
     * @see FormatFactory#forwardOptions()
     */
    default Set<ConfigOption<?>> forwardOptions() {
        return Collections.emptySet();
    }

    /** Provides catalog and session information describing the dynamic table to be accessed. */
}
