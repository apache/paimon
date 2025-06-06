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

import java.util.regex.Pattern;

/** Validator for function name. */
public class FunctionNameValidator {
    private static final Pattern FUNCTION_NAME_PATTERN =
            Pattern.compile("^(?=.*[A-Za-z])[A-Za-z0-9._-]+$");

    public static boolean validate(String name) {
        return org.apache.paimon.utils.StringUtils.isNotEmpty(name)
                && FUNCTION_NAME_PATTERN.matcher(name).matches();
    }

    public static void checkValidName(String name) {
        boolean isValid = validate(name);
        if (!isValid) {
            throw new IllegalArgumentException("Invalid function name: " + name);
        }
    }
}
