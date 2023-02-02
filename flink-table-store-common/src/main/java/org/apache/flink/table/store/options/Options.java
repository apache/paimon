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

package org.apache.flink.table.store.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Options which stores key/value pairs. */
public class Options implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Stores the concrete key/value pairs of this configuration object. */
    private final HashMap<String, String> data;

    /** Creates a new empty configuration. */
    public Options() {
        this.data = new HashMap<>();
    }

    /** Creates a new configuration that is initialized with the options of the given map. */
    public Options(Map<String, String> map) {
        this();
        map.forEach(this::setString);
    }

    public static Options fromMap(Map<String, String> map) {
        return new Options(map);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setString(String key, String value) {
        data.put(key, value);
    }

    public void set(String key, String value) {
        data.put(key, value);
    }

    public <T> void set(ConfigOption<T> key, T value) {
        Configuration conf = Configuration.fromMap(data);
        conf.set(key, value);
        data.clear();
        data.putAll(conf.toMap());
    }

    public <T> T get(ConfigOption<T> option) {
        return getOptional(option).orElseGet(option::defaultValue);
    }

    public String get(String key) {
        return data.get(key);
    }

    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        return Configuration.fromMap(data).getOptional(option);
    }

    public Set<String> keySet() {
        return data.keySet();
    }

    public Map<String, String> toMap() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Options options = (Options) o;
        return Objects.equals(data, options.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
