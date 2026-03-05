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

package org.apache.paimon.catalog;

import org.apache.paimon.annotation.Public;

import org.apache.hadoop.conf.Configuration;

/**
 * Interface for components that require Hadoop configuration.
 *
 * <p>This interface provides access to Hadoop {@link Configuration} for components that need to
 * interact with Hadoop filesystem or other Hadoop-based services.
 *
 * <p>Implementing this interface indicates that the component has a dependency on Hadoop classes.
 * Components that do not implement this interface can operate in Hadoop-free environments.
 *
 * @since 0.10.0
 */
@Public
public interface HadoopAware {

    /**
     * Returns the Hadoop configuration.
     *
     * @return Hadoop configuration instance
     */
    Configuration hadoopConf();
}
