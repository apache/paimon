# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

class TestBitSet:
    def test_set_and_get(self):
        from pypaimon.fileindex.bloom_filter import BitSet

        data = bytearray(2)  # 16 bits
        bitset = BitSet(data, 0)

        # Set bits 0, 7, 15
        bitset.set(0)
        bitset.set(7)
        bitset.set(15)

        assert bitset.get(0) is True
        assert bitset.get(1) is False
        assert bitset.get(7) is True
        assert bitset.get(15) is True
        assert bitset.get(8) is False


class TestBloomFilter64:
    def test_add_and_test_hash(self):
        from pypaimon.fileindex.bloom_filter import BloomFilter64

        # Create filter: 100 items, 0.1 fpp
        bf = BloomFilter64(100, 0.1)

        # Add hash
        bf.add_hash(12345678901234)

        # Should find it. (A bloom filter guarantees no false negatives; it does
        # NOT guarantee a non-inserted value tests False, so we only assert the
        # inserted-key invariant here.)
        assert bf.test_hash(12345678901234) is True

    def test_multiple_hashes(self):
        from pypaimon.fileindex.bloom_filter import BloomFilter64

        bf = BloomFilter64(1000, 0.01)

        # Add multiple hashes
        hashes = [111, 222, 333, 444, 555]
        for h in hashes:
            bf.add_hash(h)

        # All should be found (no false negatives). We intentionally do not
        # assert that a non-inserted value tests False — that depends on the
        # absence of a false positive, which a bloom filter does not guarantee
        # and which can flip if the sizing math changes.
        for h in hashes:
            assert bf.test_hash(h) is True
