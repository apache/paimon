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

import java.util.List;

/** FileFunctionDefinition for FunctionDefinition. */
public class FileFunctionDefinition implements FunctionDefinition {

    private final String fileType;
    private final List<String> storagePaths;
    private String language;
    private String className;
    private String functionName;

    public FileFunctionDefinition(
            String fileType,
            List<String> storagePaths,
            String language,
            String className,
            String functionName) {
        this.fileType = fileType;
        this.storagePaths = storagePaths;
        this.language = language;
        this.className = className;
        this.functionName = functionName;
    }

    @Override
    public String type() {
        return "file";
    }

    public String fileType() {
        return fileType;
    }

    public List<String> storagePaths() {
        return storagePaths;
    }

    public String language() {
        return language;
    }

    public String className() {
        return className;
    }

    public String functionName() {
        return functionName;
    }
}
