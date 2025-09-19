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

package org.apache.paimon.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/** Utils for compatibility. */
public class CompatibilityUtils {

    public static final String COMPATIBILITY_FILE_DIR = "/src/test/resources/compatibility/";

    /** Write compatibility file. */
    public static void writeCompatibilityFile(String fileName, byte[] data) throws IOException {
        File file = new File(System.getProperty("user.dir") + COMPATIBILITY_FILE_DIR + fileName);
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(data);
        }
    }
}
