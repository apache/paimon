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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.options.Options;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Timestamp;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies snapshot procedure retention against the table expiration configuration. */
class SnapshotChangelogRetentionVerificationTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testChangelogRetention(boolean independent) {
        Options options = new Options();
        options.setString("snapshot.num-retained.min", "1");
        options.setString("snapshot.num-retained.max", "100");
        options.setString("snapshot.time-retained", "2 s");
        if (independent) {
            options.setString("changelog.time-retained", "7 d");
        }
        CoreOptions table = new CoreOptions(options);
        ExpireConfig expected = table.expireConfig();
        ExpireConfig actual =
                ProcedureUtils.fillInSnapshotOptions(table, null, null, null, null).build();
        assertThat(expected.getChangelogTimeRetain())
                .isEqualTo(independent ? Duration.ofDays(7) : Duration.ofSeconds(2));
        assertThat(actual.getChangelogTimeRetain()).isEqualTo(expected.getChangelogTimeRetain());
        assertThat(actual.isChangelogDecoupled()).isEqualTo(expected.isChangelogDecoupled());
    }

    @ParameterizedTest
    @CsvSource({
        "changelog.num-retained.min, 10",
        "changelog.num-retained.max, 1000",
        "changelog.time-retained, 7 d"
    })
    void testPartialChangelogOptionsWithOverrides(String key, String value) {
        Options options = new Options();
        options.setString(key, value);
        ExpireConfig config =
                ProcedureUtils.fillInSnapshotOptions(new CoreOptions(options), 3, 1, null, 2)
                        .build();
        assertThat(config.isChangelogDecoupled()).isTrue();
        assertThat(config.getSnapshotRetainMax()).isEqualTo(3);
        assertThat(config.getSnapshotRetainMin()).isEqualTo(1);
        assertThat(config.getSnapshotMaxDeletes()).isEqualTo(2);
        assertThat(config.getChangelogRetainMax())
                .isEqualTo(key.equals("changelog.num-retained.max") ? 1000 : 3);
        assertThat(config.getChangelogRetainMin())
                .isEqualTo(key.equals("changelog.num-retained.min") ? 10 : 1);
        assertThat(config.getChangelogTimeRetain())
                .isEqualTo(
                        key.equals("changelog.time-retained")
                                ? Duration.ofDays(7)
                                : config.getSnapshotTimeRetain());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOlderThanOverride(boolean independent) {
        Options options = new Options();
        if (independent) {
            options.setString("changelog.time-retained", "7 d");
        }
        long cutoff = System.currentTimeMillis() - Duration.ofHours(1).toMillis();
        long before = System.currentTimeMillis();
        ExpireConfig config =
                ProcedureUtils.fillInSnapshotOptions(
                                new CoreOptions(options), 3, 1, new Timestamp(cutoff).toString(), 2)
                        .build();
        assertThat(config.getSnapshotTimeRetain().toMillis())
                .isBetween(before - cutoff, System.currentTimeMillis() - cutoff);
        assertThat(config.getChangelogTimeRetain())
                .isEqualTo(independent ? Duration.ofDays(7) : config.getSnapshotTimeRetain());
        assertThat(config.getChangelogRetainMax()).isEqualTo(3);
        assertThat(config.getChangelogRetainMin()).isEqualTo(1);
        assertThat(config.getChangelogMaxDeletes()).isEqualTo(2);
        assertThat(config.isChangelogDecoupled()).isEqualTo(independent);
    }
}
