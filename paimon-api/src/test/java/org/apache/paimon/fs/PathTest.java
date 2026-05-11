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

package org.apache.paimon.fs;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Path}. */
public class PathTest {

    @Test
    public void testPathWithSpecialCharacters() {
        String location = "file:/tmp/path/test file";
        String encodedLocation = "file:/tmp/path/test%20file";
        Path p1 = new Path(location);
        assertThat(p1.toString()).isEqualTo(location);
        assertThat(p1.toUri().toString()).isEqualTo(encodedLocation);

        // Using p1.toString() to create a new Path is correct
        Path p2 = new Path(p1.toString());
        assertThat(p2.toString()).isEqualTo(location);
        assertThat(p2.toUri().toString()).isEqualTo(encodedLocation);

        // Using p1.toUri().toString to create a new Path will encode again
        Path p3 = new Path(p1.toUri().toString());
        assertThat(p3.toString()).isEqualTo(encodedLocation);
        assertThat(p3.toUri().toString()).isEqualTo("file:/tmp/path/test%2520file");
    }
}
