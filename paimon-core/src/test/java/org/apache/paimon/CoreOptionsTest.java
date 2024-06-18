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

package org.apache.paimon;

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.paimon.CoreOptions}. */
public class CoreOptionsTest {

    @Test
    public void testDefaultStartupMode() {
        Options conf = new Options();
        assertThat(conf.get(CoreOptions.SCAN_MODE)).isEqualTo(CoreOptions.StartupMode.DEFAULT);
        assertThat(new CoreOptions(conf).startupMode())
                .isEqualTo(CoreOptions.StartupMode.LATEST_FULL);

        conf = new Options();
        conf.set(CoreOptions.SCAN_TIMESTAMP_MILLIS, System.currentTimeMillis());
        assertThat(new CoreOptions(conf).startupMode())
                .isEqualTo(CoreOptions.StartupMode.FROM_TIMESTAMP);

        conf = new Options();
        conf.set(CoreOptions.SCAN_TIMESTAMP, "2023-12-06 12:12:12");
        assertThat(new CoreOptions(conf).startupMode())
                .isEqualTo(CoreOptions.StartupMode.FROM_TIMESTAMP);
    }

    @Test
    public void testStartupModeCompatibility() {
        Options conf = new Options();
        conf.setString("log.scan", "latest");
        assertThat(new CoreOptions(conf).startupMode()).isEqualTo(CoreOptions.StartupMode.LATEST);

        conf = new Options();
        conf.setString("log.scan.timestamp-millis", String.valueOf(System.currentTimeMillis()));
        assertThat(new CoreOptions(conf).startupMode())
                .isEqualTo(CoreOptions.StartupMode.FROM_TIMESTAMP);
    }

    @Test
    public void testDeprecatedStartupMode() {
        Options conf = new Options();
        conf.set(CoreOptions.SCAN_MODE, CoreOptions.StartupMode.FULL);
        assertThat(new CoreOptions(conf).startupMode())
                .isEqualTo(CoreOptions.StartupMode.LATEST_FULL);
    }
}
