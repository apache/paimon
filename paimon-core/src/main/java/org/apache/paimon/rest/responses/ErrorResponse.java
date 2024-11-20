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

package org.apache.paimon.rest.responses;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

/** Response for error. */
public class ErrorResponse {
    private final String message;
    private final int code;
    private List<String> stack;

    public ErrorResponse(String message, int code, List<String> stack) {
        this.message = message;
        this.code = code;
        this.stack = stack;
    }

    public String getMessage() {
        return message;
    }

    public int getCode() {
        return code;
    }

    public List<String> getStack() {
        return stack;
    }

    public void setStack(Throwable throwable) {
        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            throwable.printStackTrace(pw);
        }

        this.stack = Arrays.asList(sw.toString().split("\n"));
    }

    public void setStack(List<String> trace) {
        this.stack = trace;
    }
}
