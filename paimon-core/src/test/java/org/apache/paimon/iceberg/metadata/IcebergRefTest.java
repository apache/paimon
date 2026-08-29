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

package org.apache.paimon.iceberg.metadata;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IcebergRef}. */
public class IcebergRefTest {

    @Test
    public void testEqualsIsByValue() {
        IcebergRef one = new IcebergRef(1L);
        IcebergRef same = new IcebergRef(1L);
        IcebergRef other = new IcebergRef(2L);

        assertThat(one).isEqualTo(same);
        assertThat(one).isNotEqualTo(other);
    }

    @Test
    public void testEqualObjectsShareAHashCode() {
        // hashCode already used value semantics, so the two disagreed: refs that hash
        // alike compared unequal.
        IcebergRef one = new IcebergRef(1L);
        IcebergRef same = new IcebergRef(1L);

        assertThat(one.hashCode()).isEqualTo(same.hashCode());

        Set<IcebergRef> set = new HashSet<>();
        set.add(one);
        set.add(same);
        assertThat(set).hasSize(1);
    }

    @Test
    public void testMetadataComparesItsRefsByValue() {
        // IcebergMetadata#equals compares refs with Objects.equals over the map, so a
        // broken element equals propagates all the way up.
        Map<String, IcebergRef> left = new HashMap<>();
        left.put("tag", new IcebergRef(1L));
        Map<String, IcebergRef> right = new HashMap<>();
        right.put("tag", new IcebergRef(1L));

        assertThat(left).isEqualTo(right);
    }
}
