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

/** Modify from parquet-hadoop to facilitate the shaded class loading. */
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

        final String shadedClassName = "org.apache.paimon.shade." + className;

        Class<?> foundClass = tryLoad(configuration, shadedClassName);
        if (foundClass == null || !assignableFrom.isAssignableFrom(foundClass)) {
            foundClass = tryLoad(configuration, className);
        }

        if (foundClass == null) {
            throw new BadConfigurationException(
                    "could not instantiate class "
                            + className
                            + " set in job conf at "
                            + configName);
        }

        if (!assignableFrom.isAssignableFrom(foundClass)) {
            throw new BadConfigurationException(
                    "class "
                            + className
                            + " set in job conf at "
                            + configName
                            + " is not a subclass of "
                            + assignableFrom.getCanonicalName());
        }

        return foundClass;
    }

    private static Class<?> tryLoad(ParquetConfiguration conf, String name) {
        try {
            return conf.getClassByName(name);
        } catch (ClassNotFoundException e) {
            return null;
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
