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

package org.apache.paimon.flink.action.cdc.kafka.format;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Represents a factory for creating instances of {@link AbstractRecordParser}.
 *
 * <p>This interface provides a method to create a new RecordParser with specific configurations
 * such as case sensitivity, table name conversion, and computed columns.
 *
 * @see AbstractRecordParser
 */
@FunctionalInterface
public interface RecordParserFactory {

    /**
     * Creates a new instance of {@link RecordParser} with the specified configurations.
     *
     * @param caseSensitive Indicates whether the parser should be case-sensitive.
     * @param typeMapping Data type mapping options.
     * @param computedColumns List of computed columns to be considered by the parser.
     * @param schemaRegistryUrl Schema registry service to manage the kafka topic message schema
     * @return A new instance of {@link RecordParser}.
     */
    RecordParser createParser(
            boolean caseSensitive,
            TypeMapping typeMapping,
            List<ComputedColumn> computedColumns,
            @Nullable String schemaRegistryUrl);
}
