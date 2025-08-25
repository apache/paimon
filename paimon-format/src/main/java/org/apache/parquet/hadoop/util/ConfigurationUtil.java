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

package org.apache.parquet.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.BadConfigurationException;

import java.util.Map;

/** Copy from parquet-hadoop to avoid shade class conflict. */
public class ConfigurationUtil {

    public static Class<?> getClassFromConfig(
            Configuration configuration, String configName, Class<?> assignableFrom) {
        return getClassFromConfig(
                new HadoopParquetConfiguration(configuration), configName, assignableFrom);
    }

    public static Class<?> getClassFromConfig(
            ParquetConfiguration configuration, String configName, Class<?> assignableFrom) {
        final String className = configuration.get(configName);
        if (className == null) {
            return null;
        }

        Class<?> foundClass;
        try {
            foundClass = configuration.getClassByName(className);
            if (!assignableFrom.isAssignableFrom(foundClass)) {
                final String paimonSharedClassName = "org.apache.paimon.shade." + className;
                foundClass = configuration.getClassByName(paimonSharedClassName);
                if (!assignableFrom.isAssignableFrom(foundClass)) {
                    throw new BadConfigurationException(
                            "class "
                                    + className
                                    + " set in job conf at "
                                    + configName
                                    + " is not a subclass of "
                                    + assignableFrom.getCanonicalName());
                }
            }
            return foundClass;
        } catch (ClassNotFoundException e) {
            throw new BadConfigurationException(
                    "could not instantiate class "
                            + className
                            + " set in job conf at "
                            + configName,
                    e);
        }
    }

    public static Configuration createHadoopConfiguration(ParquetConfiguration conf) {
        if (conf == null) {
            return new Configuration();
        }
        if (conf instanceof HadoopParquetConfiguration) {
            return ((HadoopParquetConfiguration) conf).getConfiguration();
        }
        Configuration configuration = new Configuration();
        for (Map.Entry<String, String> entry : conf) {
            configuration.set(entry.getKey(), entry.getValue());
        }
        return configuration;
    }
}
