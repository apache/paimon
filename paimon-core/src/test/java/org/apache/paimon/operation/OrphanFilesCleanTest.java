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

package org.apache.paimon.operation;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Utils for {@link OrphanFilesClean}. */
public class OrphanFilesCleanTest {

    @Test
    public void testOlderThanMillis() {
        // normal olderThan
        OrphanFilesClean.olderThanMillis(null);
        OrphanFilesClean.olderThanMillis("2024-12-21 23:00:00");

        // non normal olderThan
        assertThatThrownBy(() -> OrphanFilesClean.olderThanMillis("3024-12-21 23:00:00"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The arg olderThan must be less than now, because dataFiles that are currently being written and not referenced by snapshots will be mistakenly cleaned up.");
    }
}
