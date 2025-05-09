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

package org.apache.paimon.function;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;
import java.util.Objects;

/** Function definition. */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = FunctionDefinition.Types.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(
            value = FunctionDefinition.FileFunctionDefinition.class,
            name = FunctionDefinition.Types.FILE_TYPE),
    @JsonSubTypes.Type(
            value = FunctionDefinition.SQLFunctionDefinition.class,
            name = FunctionDefinition.Types.SQL_TYPE),
    @JsonSubTypes.Type(
            value = FunctionDefinition.LambdaFunctionDefinition.class,
            name = FunctionDefinition.Types.LAMBDA_TYPE)
})
public interface FunctionDefinition {

    static FunctionDefinition file(
            String fileType,
            List<String> storagePaths,
            String language,
            String className,
            String functionName) {
        return new FunctionDefinition.FileFunctionDefinition(
                fileType, storagePaths, language, className, functionName);
    }

    static FunctionDefinition sql(String definition) {
        return new FunctionDefinition.SQLFunctionDefinition(definition);
    }

    static FunctionDefinition lambda(String definition, String language) {
        return new FunctionDefinition.LambdaFunctionDefinition(definition, language);
    }

    /** Definition of a SQL function. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class SQLFunctionDefinition implements FunctionDefinition {

        private static final String FIELD_DEFINITION = "definition";

        private final String definition;

        public SQLFunctionDefinition(@JsonProperty(FIELD_DEFINITION) String definition) {
            this.definition = definition;
        }

        @JsonGetter(FIELD_DEFINITION)
        public String definition() {
            return definition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SQLFunctionDefinition that = (SQLFunctionDefinition) o;
            return definition.equals(that.definition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(definition);
        }
    }

    /** Lambda function definition. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class LambdaFunctionDefinition implements FunctionDefinition {

        private static final String FIELD_DEFINITION = "definition";
        private static final String FIELD_LANGUAGE = "language";

        private final String definition;
        private final String language;

        public LambdaFunctionDefinition(
                @JsonProperty(FIELD_DEFINITION) String definition,
                @JsonProperty(FIELD_LANGUAGE) String language) {
            this.definition = definition;
            this.language = language;
        }

        @JsonGetter(FIELD_DEFINITION)
        public String definition() {
            return definition;
        }

        @JsonGetter(FIELD_LANGUAGE)
        public String language() {
            return language;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LambdaFunctionDefinition that = (LambdaFunctionDefinition) o;
            return definition.equals(that.definition) && language.equals(that.language);
        }

        @Override
        public int hashCode() {
            return Objects.hash(definition, language);
        }
    }

    /** FileFunctionDefinition for FunctionDefinition. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class FileFunctionDefinition implements FunctionDefinition {

        private static final String FIELD_FILE_TYPE = "fileType";
        private static final String FIELD_STORAGE_PATHS = "storagePaths";
        private static final String FIELD_LANGUAGE = "language";
        private static final String FIELD_CLASS_NAME = "className";
        private static final String FIELD_FUNCTION_NAME = "functionName";

        @JsonProperty(FIELD_FILE_TYPE)
        private final String fileType;

        @JsonProperty(FIELD_STORAGE_PATHS)
        private final List<String> storagePaths;

        @JsonProperty(FIELD_LANGUAGE)
        private String language;

        @JsonProperty(FIELD_CLASS_NAME)
        private String className;

        @JsonProperty(FIELD_FUNCTION_NAME)
        private String functionName;

        public FileFunctionDefinition(
                @JsonProperty(FIELD_FILE_TYPE) String fileType,
                @JsonProperty(FIELD_STORAGE_PATHS) List<String> storagePaths,
                @JsonProperty(FIELD_LANGUAGE) String language,
                @JsonProperty(FIELD_CLASS_NAME) String className,
                @JsonProperty(FIELD_FUNCTION_NAME) String functionName) {
            this.fileType = fileType;
            this.storagePaths = storagePaths;
            this.language = language;
            this.className = className;
            this.functionName = functionName;
        }

        @JsonGetter(FIELD_FILE_TYPE)
        public String fileType() {
            return fileType;
        }

        @JsonGetter(FIELD_STORAGE_PATHS)
        public List<String> storagePaths() {
            return storagePaths;
        }

        @JsonGetter(FIELD_LANGUAGE)
        public String language() {
            return language;
        }

        @JsonGetter(FIELD_CLASS_NAME)
        public String className() {
            return className;
        }

        @JsonGetter(FIELD_FUNCTION_NAME)
        public String functionName() {
            return functionName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FileFunctionDefinition that = (FileFunctionDefinition) o;
            return fileType.equals(that.fileType)
                    && Objects.equals(storagePaths, that.storagePaths)
                    && Objects.equals(language, that.language)
                    && Objects.equals(className, that.className)
                    && Objects.equals(functionName, that.functionName);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(fileType, language, className, functionName);
            result = 31 * result + Objects.hashCode(storagePaths);
            return result;
        }
    }

    /** Types for FunctionDefinition. */
    class Types {
        public static final String FIELD_TYPE = "type";
        public static final String FILE_TYPE = "file";
        public static final String SQL_TYPE = "sql";
        public static final String LAMBDA_TYPE = "lambda";

        private Types() {}
    }
}
