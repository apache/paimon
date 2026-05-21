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

package org.apache.paimon.fileindex.rtree;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RTreeTest {

    @Test
    void testRTreeInsertion() {
        RTree rtree = new RTree(2, 4);

        rtree.insert(new double[] {0.0, 0.0}, 0);
        rtree.insert(new double[] {5.0, 5.0}, 1);
        rtree.insert(new double[] {10.0, 10.0}, 2);

        assertThat(rtree.getSize()).isEqualTo(3);
    }

    @Test
    void testRTreeSearch() {
        RTree rtree = new RTree(2, 8);

        rtree.insert(new double[] {0.0, 0.0}, 0);
        rtree.insert(new double[] {5.0, 5.0}, 1);
        rtree.insert(new double[] {10.0, 10.0}, 2);
        rtree.insert(new double[] {15.0, 15.0}, 3);

        BoundingBox searchBox = new BoundingBox(new double[] {3.0, 3.0}, new double[] {7.0, 7.0});
        List<Integer> results = rtree.search(searchBox);

        assertThat(results).contains(1);
    }

    @Test
    void testRTreePointSearch() {
        RTree rtree = new RTree(2, 8);

        rtree.insert(new double[] {0.0, 0.0}, 0);
        rtree.insert(new double[] {5.0, 5.0}, 1);
        rtree.insert(new double[] {10.0, 10.0}, 2);

        List<Integer> results = rtree.search(new double[] {5.0, 5.0});

        assertThat(results).contains(1);
    }

    @Test
    void testRTreeLargeInsertion() {
        RTree rtree = new RTree(2, 8);

        for (int i = 0; i < 100; i++) {
            double x = i % 10;
            double y = i / 10;
            rtree.insert(new double[] {x, y}, i);
        }

        assertThat(rtree.getSize()).isEqualTo(100);

        BoundingBox searchBox = new BoundingBox(new double[] {0.0, 0.0}, new double[] {5.0, 5.0});
        List<Integer> results = rtree.search(searchBox);

        assertThat(results).isNotEmpty();
    }
}
