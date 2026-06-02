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

"""Cross-language compatibility tests for the bloom-filter file index.

The PyPaimon read path must decode bloom-filter indexes written by the Java
writer, so the hash arithmetic has to reproduce Java's integer-overflow
semantics bit-for-bit. These unit tests pin each routine with vectors derived
from the Java algorithms (Thomas Wang long overflow, XXH64 constants) and the
on-disk format rules, to localize any drift to a specific routine.

End-to-end byte compatibility against the real Java writer is covered separately
by the e2e test ``JavaPyE2ETest#testBloomFilterIndexWrite`` ->
``JavaPyReadWriteTest::test_read_bloom_filter_index_table``.
"""

import pytest


class TestGetLongHashJavaCompat:
    """get_long_hash must match Java FastHash.getLongHash for all 64-bit keys."""

    # key -> Java getLongHash(key) reinterpreted as unsigned 64-bit
    GOLDEN = {
        0: 0,
        1: 6614235796240398542,
        2: 13228483051453548148,
        42: 1098236396662648698,
        123456789: 16581954974024456952,
        # 2**40 - regression: the original port computed this wrong because it
        # never truncated intermediate results to 64 bits.
        1099511627776: 6024996419662910741,
        # signed boundaries (passed in as unsigned 64-bit)
        (1 << 64) - 1: 6614246905173314819,            # Java -1L
        9223372036854775807: 9344215449356265527,       # Long.MAX_VALUE
        9223372036854775808: 4316648529147585864,       # Long.MIN_VALUE bits
    }

    def test_known_vectors(self):
        from pypaimon.fileindex.fast_hash import get_long_hash

        for key, expected in self.GOLDEN.items():
            assert get_long_hash(key) == expected, f"key={key}"


class TestXxHashJavaCompat:
    """hash64_bytes must match Java net.openhft LongHashFunction.xx() (XXH64 seed=0)."""

    GOLDEN = {
        b"": 17241709254077376921,
        b"a": 15154266338359012955,
        b"hello": 2794345569481354659,
        b"paimon": 615975309333239268,
    }

    def test_known_vectors(self):
        pytest.importorskip("xxhash")
        from pypaimon.fileindex.fast_hash import hash64_bytes

        for data, expected in self.GOLDEN.items():
            assert hash64_bytes(data) == expected, f"data={data!r}"


class TestTypeRouting:
    """Type dispatch must not misroute TIMESTAMP/TIME and must reach numeric/string branches."""

    def _atomic(self, type_str):
        from pypaimon.schema.data_types import AtomicType

        return AtomicType(type_str)

    def test_timestamp_not_routed_to_integer(self):
        # TIMESTAMP must use the millisecond/micros logic, NOT int(value).
        # A bare int() on a timestamp-like object would raise; the dedicated
        # branch handles get_millisecond()/to_micros() (as the real pypaimon
        # Timestamp does).
        from pypaimon.fileindex.fast_hash import get_hash_function, get_long_hash

        class FakeTs:
            def get_millisecond(self):
                return 1700000000000

            def to_micros(self):
                return 1700000000000000

        hash_fn = get_hash_function(self._atomic("TIMESTAMP(3)"))
        assert hash_fn(FakeTs()) == get_long_hash(1700000000000)

    def test_int_routes_to_long_hash(self):
        from pypaimon.fileindex.fast_hash import get_hash_function, get_long_hash

        hash_fn = get_hash_function(self._atomic("INT"))
        assert hash_fn(42) == get_long_hash(42)

    def test_tinyint_in_range_matches_plain_hash(self):
        from pypaimon.fileindex.fast_hash import get_hash_function, get_long_hash

        # In-range values (the only ones pyarrow produces) are unchanged.
        hash_fn = get_hash_function(self._atomic("TINYINT"))
        for v in (0, 1, -1, 127, -128):
            assert hash_fn(v) == get_long_hash(v), f"value={v}"

    def test_tinyint_out_of_range_sign_truncates_like_java(self):
        from pypaimon.fileindex.fast_hash import get_hash_function, get_long_hash

        # Java casts to (byte): 200 -> -56, 256 -> 0.
        hash_fn = get_hash_function(self._atomic("TINYINT"))
        assert hash_fn(200) == get_long_hash(-56)
        assert hash_fn(256) == get_long_hash(0)

    def test_smallint_out_of_range_sign_truncates_like_java(self):
        from pypaimon.fileindex.fast_hash import get_hash_function, get_long_hash

        # Java casts to (short): 40000 -> 40000 - 65536 = -25536.
        hash_fn = get_hash_function(self._atomic("SMALLINT"))
        assert hash_fn(40000) == get_long_hash(-25536)

    def test_varchar_routes_to_bytes_hash(self):
        pytest.importorskip("xxhash")
        from pypaimon.fileindex.fast_hash import get_hash_function, hash64_bytes

        hash_fn = get_hash_function(self._atomic("VARCHAR(10)"))
        assert hash_fn("hello") == hash64_bytes(b"hello")


class TestEvaluatorFailOpen:
    """The predicate evaluator must fail open (REMAIN) on a literal it cannot
    hash in a Java-compatible way, rather than raise. This is pure-Python
    evaluator behavior, so it builds its own bloom blob locally."""

    def _bigint_predicate(self):
        from pypaimon.fileindex.file_index_predicate import FileIndexPredicate
        from pypaimon.schema.data_types import AtomicType
        from pypaimon.tests.fileindex.bloom_blob_builder import build_bloom_blob

        blob = build_bloom_blob("a", "BIGINT", (10, 20, 30))
        return FileIndexPredicate(blob, {"a": AtomicType("BIGINT")})

    @staticmethod
    def _equal(value):
        from pypaimon.common.predicate import Predicate

        return Predicate(method="equal", index=0, field="a", literals=[value])

    @staticmethod
    def _in(values):
        from pypaimon.common.predicate import Predicate

        return Predicate(method="in", index=0, field="a", literals=values)

    def test_present_value_remains(self):
        fip = self._bigint_predicate()
        assert fip.evaluate(self._equal(20)).remain() is True

    def test_absent_value_skips(self):
        fip = self._bigint_predicate()
        assert fip.evaluate(self._equal(99)).remain() is False

    def test_unhashable_literal_fails_open(self):
        # A non-numeric string against a BIGINT column (as a CLI WHERE parser
        # might produce) cannot be hashed, so the evaluator must fail open to
        # REMAIN rather than raise.
        fip = self._bigint_predicate()
        assert fip.evaluate(self._equal("not-a-number")).remain() is True
        assert fip.evaluate(self._in(["nope", "also-nope"])).remain() is True

    def test_empty_in_fails_open(self):
        from pypaimon.common.predicate import Predicate

        # An empty IN has no value to probe; it must fail open (REMAIN) rather
        # than SKIP, which would silently drop the file.
        fip = self._bigint_predicate()
        empty_in = Predicate(method="in", index=0, field="a", literals=[])
        assert fip.evaluate(empty_in).remain() is True

    def test_empty_or_fails_open(self):
        from pypaimon.common.predicate import Predicate

        # An empty OR likewise must fail open rather than SKIP.
        fip = self._bigint_predicate()
        empty_or = Predicate(method="or", index=None, field=None, literals=[])
        assert fip.evaluate(empty_or).remain() is True


class TestTemporalHashing:
    """DATE/TIME/TIMESTAMP literals must hash their Java internal representation.

    Java's FastHash hashes DATE as epoch-day (int), TIME as millis-of-day (int),
    and TIMESTAMP as millis (precision<=3) or micros (precision>3). The Python
    read path receives native ``date``/``time``/``datetime`` literals, so it must
    convert them before hashing — otherwise the bloom probe lands on the wrong
    bits and a present value can be wrongly skipped.
    """

    def _atomic(self, type_str):
        from pypaimon.schema.data_types import AtomicType

        return AtomicType(type_str)

    def test_date_hashes_epoch_day(self):
        from datetime import date
        from pypaimon.fileindex.fast_hash import get_hash_function, get_long_hash

        d = date(2021, 1, 1)
        epoch_day = (d - date(1970, 1, 1)).days
        assert get_hash_function(self._atomic("DATE"))(d) == get_long_hash(epoch_day)

    def test_time_hashes_millis_of_day(self):
        from datetime import time
        from pypaimon.fileindex.fast_hash import get_hash_function, get_long_hash

        t = time(1, 2, 3, 456000)
        millis = (1 * 3600 + 2 * 60 + 3) * 1000 + 456000 // 1000
        assert get_hash_function(self._atomic("TIME(3)"))(t) == get_long_hash(millis)

    def test_timestamp_precision3_hashes_millis(self):
        import calendar
        from datetime import datetime
        from pypaimon.fileindex.fast_hash import get_hash_function, get_long_hash

        ts = datetime(2021, 1, 1, 0, 0, 0)
        millis = calendar.timegm(ts.timetuple()) * 1000
        assert get_hash_function(self._atomic("TIMESTAMP(3)"))(ts) == get_long_hash(millis)

    def test_timestamp_precision6_hashes_micros(self):
        import calendar
        from datetime import datetime
        from pypaimon.fileindex.fast_hash import get_hash_function, get_long_hash

        ts = datetime(2021, 1, 1, 0, 0, 0)
        micros = calendar.timegm(ts.timetuple()) * 1_000_000
        assert get_hash_function(self._atomic("TIMESTAMP(6)"))(ts) == get_long_hash(micros)

    def test_timestamp_rejects_ambiguous_int(self):
        from pypaimon.fileindex.fast_hash import get_hash_function

        # A bare int cannot be hashed unambiguously (millis vs micros), so it
        # must raise rather than silently guess the wrong unit.
        with pytest.raises(TypeError):
            get_hash_function(self._atomic("TIMESTAMP(3)"))(12345)

    def test_temporal_none_hashes_zero(self):
        from pypaimon.fileindex.fast_hash import get_hash_function

        for type_str in ("DATE", "TIME(3)", "TIMESTAMP(3)", "TIMESTAMP(6)"):
            assert get_hash_function(self._atomic(type_str))(None) == 0

    def test_timezone_aware_datetime_raises(self):
        from datetime import datetime, timezone, timedelta
        from pypaimon.fileindex.fast_hash import get_hash_function

        # An aware datetime carries an instant the wall-clock hash would get
        # wrong, so it must raise (the evaluator then fails open) rather than
        # silently probe the wrong bloom bit.
        aware = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=8)))
        for type_str in ("TIMESTAMP(3)", "TIMESTAMP(6)"):
            with pytest.raises(TypeError):
                get_hash_function(self._atomic(type_str))(aware)


class TestFileIndexFormatHardening:
    """The format reader must mirror Java's signed-int / EOF semantics so a
    corrupt or empty index degrades safely (REMAIN) instead of silently
    producing a short bitset that could cause false SKIPs."""

    def _make_reader(self, blob):
        from pypaimon.fileindex.file_index_format import FileIndexFormatReader

        return FileIndexFormatReader(blob)

    def _header_blob(self, start_pos, length, body=b""):
        """Build a one-column ('a') / one-index ('bloom-filter') blob with an
        arbitrary (start_pos, length) so the reader's bounds handling can be
        exercised. ``start_pos`` is written signed, matching Java."""
        import struct

        from pypaimon.fileindex.file_index_format import MAGIC, VERSION_1

        def w_utf(s):
            b = s.encode("utf-8")
            return struct.pack(">H", len(b)) + b

        col, idx = "a", "bloom-filter"
        head_len = 8 + 4 + 4 + (4 + len(w_utf(col)) + 4 + len(w_utf(idx)) + 4 + 4 + 4)
        head = (
            struct.pack(">I", 1)
            + w_utf(col)
            + struct.pack(">I", 1)
            + w_utf(idx)
            + struct.pack(">i", start_pos)   # signed, like Java writeInt
            + struct.pack(">i", length)
            + struct.pack(">I", 0)            # redundant length
        )
        return (
            struct.pack(">Q", MAGIC)
            + struct.pack(">I", VERSION_1)
            + struct.pack(">I", head_len)
            + head
            + body
        )

    def test_empty_index_flag_returns_none(self):
        # Java writes EMPTY_INDEX_FLAG (-1) for columns with no index body.
        blob = self._header_blob(start_pos=-1, length=0)
        reader = self._make_reader(blob)
        assert reader.get_columns() == ["a"]
        assert reader.read_column_index("a", "bloom-filter") is None

    def test_out_of_range_slice_returns_none(self):
        # A truncated/corrupt blob whose declared (start,length) exceeds the data
        # must NOT return a silently-shortened slice.
        blob = self._header_blob(start_pos=10_000, length=64)
        reader = self._make_reader(blob)
        assert reader.read_column_index("a", "bloom-filter") is None

    def test_length_overrun_returns_none(self):
        blob = self._header_blob(start_pos=0, length=1)  # start 0 is inside header, but
        # extend length far past the end to force the overrun branch
        blob = self._header_blob(start_pos=len(blob) - 4, length=1024)
        reader = self._make_reader(blob)
        assert reader.read_column_index("a", "bloom-filter") is None
