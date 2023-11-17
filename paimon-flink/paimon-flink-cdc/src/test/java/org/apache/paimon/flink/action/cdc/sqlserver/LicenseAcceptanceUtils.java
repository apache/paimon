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

package org.apache.paimon.flink.action.cdc.sqlserver;

import org.testcontainers.shaded.com.google.common.base.Charsets;
import org.testcontainers.shaded.com.google.common.io.Resources;

import java.net.URL;
import java.util.List;
import java.util.stream.Stream;

/** Copy from org.testcontainers.utility.LicenseAcceptance, modify license location. */
public final class LicenseAcceptanceUtils {
    private static final String ACCEPTANCE_FILE_NAME =
            "META-INF/licenses/container-license-acceptance.txt";

    public static void assertLicenseAccepted(String imageName) {
        try {
            URL url = Resources.getResource(ACCEPTANCE_FILE_NAME);
            List<String> acceptedLicences = Resources.readLines(url, Charsets.UTF_8);
            Stream licenses = acceptedLicences.stream().map(String::trim);
            imageName.getClass();
            if (licenses.anyMatch(imageName::equals)) {
                return;
            }
        } catch (Exception exception) {
        }

        throw new IllegalStateException(
                "The image "
                        + imageName
                        + " requires you to accept a license agreement. Please place a file at the root of the classpath named "
                        + "META-INF/licenses/container-license-acceptance.txt"
                        + ", e.g. at src/test/resources/"
                        + "META-INF/licenses/container-license-acceptance.txt"
                        + ". This file should contain the line:\n  "
                        + imageName);
    }

    private LicenseAcceptanceUtils() {
        throw new UnsupportedOperationException(
                "This is a utility class and cannot be instantiated");
    }
}
