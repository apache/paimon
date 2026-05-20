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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class BoundingBoxTest {

    @Test
    void testBoundingBoxCreation() {
        double[] min = {0.0, 0.0};
        double[] max = {10.0, 10.0};
        BoundingBox bbox = new BoundingBox(min, max);

        assertThat(bbox.getMin()).isEqualTo(min);
        assertThat(bbox.getMax()).isEqualTo(max);
        assertThat(bbox.getDimensions()).isEqualTo(2);
    }

    @Test
    void testBoundingBoxExpand() {
        BoundingBox bbox1 = new BoundingBox(new double[] {0.0, 0.0}, new double[] {10.0, 10.0});
        BoundingBox bbox2 = new BoundingBox(new double[] {5.0, 5.0}, new double[] {15.0, 15.0});

        bbox1.expand(bbox2);

        assertThat(bbox1.getMin()).isEqualTo(new double[] {0.0, 0.0});
        assertThat(bbox1.getMax()).isEqualTo(new double[] {15.0, 15.0});
    }

    @Test
    void testBoundingBoxIntersects() {
        BoundingBox bbox1 = new BoundingBox(new double[] {0.0, 0.0}, new double[] {10.0, 10.0});
        BoundingBox bbox2 = new BoundingBox(new double[] {5.0, 5.0}, new double[] {15.0, 15.0});
        BoundingBox bbox3 = new BoundingBox(new double[] {20.0, 20.0}, new double[] {30.0, 30.0});

        assertThat(bbox1.intersects(bbox2)).isTrue();
        assertThat(bbox1.intersects(bbox3)).isFalse();
    }

    @Test
    void testBoundingBoxContains() {
        BoundingBox bbox = new BoundingBox(new double[] {0.0, 0.0}, new double[] {10.0, 10.0});

        assertThat(bbox.contains(new double[] {5.0, 5.0})).isTrue();
        assertThat(bbox.contains(new double[] {15.0, 15.0})).isFalse();
    }

    @Test
    void testBoundingBoxArea() {
        BoundingBox bbox = new BoundingBox(new double[] {0.0, 0.0}, new double[] {10.0, 10.0});
        assertThat(bbox.getArea()).isEqualTo(100.0);
    }

    @Test
    void testBoundingBoxSerialization() throws Exception {
        BoundingBox bbox = new BoundingBox(new double[] {0.0, 0.0}, new double[] {10.0, 10.0});

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        bbox.serialize(dos);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        BoundingBox deserialized = BoundingBox.deserialize(dis, 2);

        assertThat(deserialized.getMin()).isEqualTo(bbox.getMin());
        assertThat(deserialized.getMax()).isEqualTo(bbox.getMax());
    }
}
