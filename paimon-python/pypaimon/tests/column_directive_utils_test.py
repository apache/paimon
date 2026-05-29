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

import unittest

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.column_directive_utils import (
    apply_add_column_directive,
    apply_directives,
    parse_add_column_comment,
    remove_dropped_directive_options,
)
from pypaimon.schema.data_types import (
    ArrayType, AtomicType, DataField, VectorType,
)


class TestParseAddColumnComment(unittest.TestCase):

    def test_none_and_empty(self):
        self.assertIsNone(parse_add_column_comment(None))
        self.assertIsNone(parse_add_column_comment(""))
        self.assertIsNone(parse_add_column_comment("normal comment"))

    def test_blob_field(self):
        d = parse_add_column_comment("__BLOB_FIELD; picture")
        self.assertEqual(d.option_key, CoreOptions.BLOB_FIELD.key())
        self.assertEqual(d.real_comment, "picture")
        self.assertFalse(d.is_vector)

    def test_blob_field_bare(self):
        d = parse_add_column_comment("__BLOB_FIELD")
        self.assertIsNone(d.real_comment)

    def test_blob_descriptor_field(self):
        d = parse_add_column_comment("__BLOB_DESCRIPTOR_FIELD; desc")
        self.assertEqual(d.option_key, CoreOptions.BLOB_DESCRIPTOR_FIELD.key())

    def test_blob_view_field(self):
        d = parse_add_column_comment("__BLOB_VIEW_FIELD; view")
        self.assertEqual(d.option_key, CoreOptions.BLOB_VIEW_FIELD.key())

    def test_blob_external_storage_field(self):
        d = parse_add_column_comment("__BLOB_EXTERNAL_STORAGE_FIELD; ext")
        self.assertEqual(d.option_key, CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key())

    def test_vector_field(self):
        d = parse_add_column_comment("__VECTOR_FIELD;128; embedding")
        self.assertEqual(d.option_key, CoreOptions.VECTOR_FIELD.key())
        self.assertTrue(d.is_vector)
        self.assertEqual(d.vector_dim, 128)
        self.assertEqual(d.real_comment, "embedding")

    def test_vector_field_no_comment(self):
        d = parse_add_column_comment("__VECTOR_FIELD;64")
        self.assertEqual(d.vector_dim, 64)
        self.assertIsNone(d.real_comment)

    def test_unknown_blob_directive_rejected(self):
        with self.assertRaises(ValueError):
            parse_add_column_comment("__BLOB_UNKNOWN")

    def test_vector_without_dim_rejected(self):
        with self.assertRaises(ValueError):
            parse_add_column_comment("__VECTOR_FIELD")

    def test_vector_non_integer_dim_rejected(self):
        with self.assertRaises(ValueError):
            parse_add_column_comment("__VECTOR_FIELD;abc")


class TestApplyAddColumnDirective(unittest.TestCase):

    def test_non_directive_returns_none(self):
        opts = {}
        result = apply_add_column_directive(
            "normal", "col", AtomicType("BYTES"), opts
        )
        self.assertIsNone(result)
        self.assertEqual(opts, {})

    def test_blob_field(self):
        opts = {}
        result = apply_add_column_directive(
            "__BLOB_FIELD; pic", "pic", AtomicType("BYTES"), opts
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.type.type, "BLOB")
        self.assertEqual(result.comment, "pic")
        self.assertEqual(opts[CoreOptions.BLOB_FIELD.key()], "pic")

    def test_vector_field(self):
        opts = {}
        result = apply_add_column_directive(
            "__VECTOR_FIELD;128; emb",
            "emb",
            ArrayType(True, AtomicType("FLOAT")),
            opts
        )
        self.assertIsNotNone(result)
        self.assertIsInstance(result.type, VectorType)
        self.assertEqual(result.type.length, 128)
        self.assertEqual(result.comment, "emb")
        self.assertEqual(opts[CoreOptions.VECTOR_FIELD.key()], "emb")

    def test_external_storage_registers_both(self):
        opts = {}
        apply_add_column_directive(
            "__BLOB_EXTERNAL_STORAGE_FIELD", "vid", AtomicType("BYTES"), opts
        )
        self.assertEqual(opts[CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key()], "vid")
        self.assertEqual(opts[CoreOptions.BLOB_DESCRIPTOR_FIELD.key()], "vid")

    def test_blob_rejects_non_binary(self):
        with self.assertRaises(ValueError):
            apply_add_column_directive(
                "__BLOB_FIELD", "col", AtomicType("INT"), {}
            )

    def test_vector_rejects_non_array(self):
        with self.assertRaises(ValueError):
            apply_add_column_directive(
                "__VECTOR_FIELD;128", "col", AtomicType("INT"), {}
            )

    def test_appends_to_existing(self):
        opts = {CoreOptions.BLOB_FIELD.key(): "a"}
        apply_add_column_directive(
            "__BLOB_FIELD", "b", AtomicType("BYTES"), opts
        )
        self.assertEqual(opts[CoreOptions.BLOB_FIELD.key()], "a,b")

    def test_migrates_legacy_fallback_key(self):
        opts = {"blob.stored-descriptor-fields": "legacy_col"}
        apply_add_column_directive(
            "__BLOB_DESCRIPTOR_FIELD", "new_col", AtomicType("BYTES"), opts
        )
        self.assertEqual(
            opts[CoreOptions.BLOB_DESCRIPTOR_FIELD.key()], "legacy_col,new_col"
        )
        self.assertNotIn("blob.stored-descriptor-fields", opts)


class TestApplyDirectives(unittest.TestCase):

    def test_no_directives(self):
        fields = [DataField(0, "k", AtomicType("INT"))]
        opts = {}
        changed = apply_directives(fields, opts)
        self.assertFalse(changed)
        self.assertEqual(fields[0].type.type, "INT")

    def test_mixed_fields(self):
        fields = [
            DataField(0, "k", AtomicType("INT")),
            DataField(1, "pic", AtomicType("BYTES"), "__BLOB_FIELD; picture"),
            DataField(
                2, "emb", ArrayType(True, AtomicType("FLOAT")),
                "__VECTOR_FIELD;64; my emb"
            ),
        ]
        opts = {}
        changed = apply_directives(fields, opts)
        self.assertTrue(changed)
        self.assertEqual(fields[1].type.type, "BLOB")
        self.assertEqual(fields[1].description, "picture")
        self.assertIsInstance(fields[2].type, VectorType)
        self.assertEqual(fields[2].type.length, 64)
        self.assertEqual(fields[2].description, "my emb")
        self.assertEqual(opts[CoreOptions.BLOB_FIELD.key()], "pic")
        self.assertEqual(opts[CoreOptions.VECTOR_FIELD.key()], "emb")


class TestRemoveDroppedDirectiveOptions(unittest.TestCase):

    def test_drop_blob(self):
        opts = {
            CoreOptions.BLOB_FIELD.key(): "a,b",
            CoreOptions.BLOB_DESCRIPTOR_FIELD.key(): "b",
            CoreOptions.VECTOR_FIELD.key(): "v",
        }
        remove_dropped_directive_options("b", "BLOB", opts)
        self.assertEqual(opts[CoreOptions.BLOB_FIELD.key()], "a")
        self.assertNotIn(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), opts)
        self.assertEqual(opts[CoreOptions.VECTOR_FIELD.key()], "v")

    def test_drop_vector(self):
        opts = {
            CoreOptions.VECTOR_FIELD.key(): "emb,emb2",
            "field.emb.vector-dim": "128",
            CoreOptions.BLOB_FIELD.key(): "a",
        }
        remove_dropped_directive_options("emb", "VECTOR", opts)
        self.assertEqual(opts[CoreOptions.VECTOR_FIELD.key()], "emb2")
        self.assertNotIn("field.emb.vector-dim", opts)
        self.assertEqual(opts[CoreOptions.BLOB_FIELD.key()], "a")

    def test_drop_blob_cleans_fallback_keys(self):
        opts = {
            CoreOptions.BLOB_DESCRIPTOR_FIELD.key(): "b,c",
            "blob.stored-descriptor-fields": "b,legacy",
        }
        remove_dropped_directive_options("b", "BLOB", opts)
        self.assertEqual(opts[CoreOptions.BLOB_DESCRIPTOR_FIELD.key()], "c")
        self.assertEqual(opts["blob.stored-descriptor-fields"], "legacy")

    def test_drop_non_directive_is_noop(self):
        opts = {CoreOptions.BLOB_FIELD.key(): "a"}
        remove_dropped_directive_options("x", "INT", opts)
        self.assertEqual(opts[CoreOptions.BLOB_FIELD.key()], "a")


if __name__ == '__main__':
    unittest.main()
