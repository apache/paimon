################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
"""
Tests for Identifier parsing, including backtick support for database names with periods.
"""

import unittest
import warnings

from pypaimon.common.identifier import Identifier


class IdentifierTest(unittest.TestCase):
    """Tests for Identifier.from_string()."""

    def test_simple_identifier(self):
        """Simple database.table parsing."""
        identifier = Identifier.from_string("mydb.mytable")
        self.assertEqual(identifier.database, "mydb")
        self.assertEqual(identifier.object, "mytable")

    def test_java_compatible_split_on_first_period(self):
        """Java-compatible: splits on first period only, allowing periods in table name."""
        identifier = Identifier.from_string("mydb.my.table.name")
        self.assertEqual(identifier.database, "mydb")
        self.assertEqual(identifier.object, "my.table.name")

    def test_backtick_quoted_database_name_with_period(self):
        """Backtick-quoted database name containing a period."""
        identifier = Identifier.from_string("`db.name`.table_name")
        self.assertEqual(identifier.database, "db.name")
        self.assertEqual(identifier.object, "table_name")

    def test_backtick_quoted_both_parts(self):
        """Both database and table names backtick-quoted."""
        identifier = Identifier.from_string("`db.name`.`table.name`")
        self.assertEqual(identifier.database, "db.name")
        self.assertEqual(identifier.object, "table.name")

    def test_backtick_quoted_database_only(self):
        """Only database name backtick-quoted."""
        identifier = Identifier.from_string("`my.database`.simple_table")
        self.assertEqual(identifier.database, "my.database")
        self.assertEqual(identifier.object, "simple_table")

    def test_get_full_name(self):
        """get_full_name() returns database.object format."""
        identifier = Identifier.create("mydb", "mytable")
        self.assertEqual(identifier.get_full_name(), "mydb.mytable")

    def test_get_full_name_with_branch_encoded_in_object(self):
        """A branch is encoded into ``object`` (Java-compatible)."""
        identifier = Identifier.create("mydb", "mytable", branch="feature")
        self.assertEqual(identifier.object, "mytable$branch_feature")
        self.assertEqual(identifier.get_full_name(), "mydb.mytable$branch_feature")
        self.assertEqual(identifier.get_table_name(), "mytable")
        self.assertEqual(identifier.get_branch_name(), "feature")
        self.assertEqual(identifier.get_branch_name_or_default(), "feature")
        self.assertFalse(identifier.is_system_table())

    def test_main_branch_is_not_encoded_into_object(self):
        """``main`` (case-insensitive) is the default branch and is not encoded into ``object``.

        Note: matching Java semantics, the cached branch value passed to
        ``create`` is preserved on the instance, so ``get_branch_name()``
        returns the supplied string. The identifier is still wire-equal to
        a no-branch one because only ``object`` round-trips through JSON.
        """
        for branch in ("main", "MAIN", "Main"):
            identifier = Identifier.create("mydb", "mytable", branch=branch)
            self.assertEqual(identifier.object, "mytable")
            # Wire-equal to a no-branch identifier.
            self.assertEqual(identifier, Identifier.create("mydb", "mytable"))

    def test_get_branch_name_or_default_when_unset(self):
        """``get_branch_name_or_default`` falls back to 'main'."""
        identifier = Identifier.create("mydb", "mytable")
        self.assertIsNone(identifier.get_branch_name())
        self.assertEqual(identifier.get_branch_name_or_default(), "main")

    def test_unknown_database_drops_database_segment(self):
        """``UNKNOWN_DATABASE`` is dropped from full name (Java-compatible)."""
        identifier = Identifier("unknown", "mytable")
        self.assertEqual(identifier.get_full_name(), "mytable")

    def test_empty_string_raises_error(self):
        """Empty string should raise ValueError."""
        with self.assertRaises(ValueError):
            Identifier.from_string("")

    def test_whitespace_only_raises_error(self):
        """Whitespace-only string should raise ValueError."""
        with self.assertRaises(ValueError):
            Identifier.from_string("   ")

    def test_no_period_raises_error(self):
        """String without period should raise ValueError."""
        with self.assertRaises(ValueError):
            Identifier.from_string("nodothere")

    def test_unclosed_backtick_raises_error(self):
        """Unclosed backtick should raise ValueError."""
        with self.assertRaises(ValueError):
            Identifier.from_string("`unclosed.db.mytable")

    def test_invalid_backtick_format_raises_error(self):
        """Invalid backtick format (too many parts) should raise ValueError."""
        with self.assertRaises(ValueError):
            Identifier.from_string("`a`.`b`.`c`")

    def test_is_system_table_regular_table(self):
        """A plain table object is not a system table."""
        self.assertFalse(Identifier.create("mydb", "mytable").is_system_table())

    def test_is_system_table_snapshots_suffix(self):
        """object name '<base>$snapshots' is a system table."""
        self.assertTrue(Identifier("mydb", "orders$snapshots").is_system_table())
        self.assertTrue(
            Identifier.create("mydb", "orders", system_table="snapshots").is_system_table())

    def test_is_system_table_schemas_suffix(self):
        """object name '<base>$schemas' is a system table."""
        self.assertTrue(Identifier("mydb", "orders$schemas").is_system_table())

    def test_is_system_table_files_suffix(self):
        """object name '<base>$files' is a system table."""
        self.assertTrue(Identifier("mydb", "orders$files").is_system_table())

    def test_is_system_table_two_parts_with_branch_prefix(self):
        """A '<table>$branch_<name>' object is a branched table, NOT a system table."""
        identifier = Identifier("mydb", "orders$branch_dev")
        self.assertFalse(identifier.is_system_table())
        self.assertEqual(identifier.get_table_name(), "orders")
        self.assertEqual(identifier.get_branch_name(), "dev")
        self.assertIsNone(identifier.get_system_table_name())

    def test_is_system_table_three_parts_branch_and_system(self):
        """A '<table>$branch_<name>$snapshots' object is a system table on a branch."""
        identifier = Identifier("mydb", "orders$branch_dev$snapshots")
        self.assertTrue(identifier.is_system_table())
        self.assertEqual(identifier.get_table_name(), "orders")
        self.assertEqual(identifier.get_branch_name(), "dev")
        self.assertEqual(identifier.get_system_table_name(), "snapshots")

    def test_three_parts_without_branch_prefix_raises(self):
        """A '<table>$<x>$<y>' object without branch prefix is invalid (single '$' allowed)."""
        identifier = Identifier("mydb", "orders$schemas$snapshots")
        with self.assertRaises(ValueError):
            identifier.is_system_table()

    def test_create_with_branch_and_system_table(self):
        """``create`` encodes both branch and system_table into the object name."""
        identifier = Identifier.create(
            "mydb", "orders", branch="dev", system_table="snapshots"
        )
        self.assertEqual(identifier.object, "orders$branch_dev$snapshots")
        self.assertEqual(identifier.get_table_name(), "orders")
        self.assertEqual(identifier.get_branch_name(), "dev")
        self.assertEqual(identifier.get_system_table_name(), "snapshots")

    def test_create_with_system_table_only(self):
        """``create`` with system_table but no branch."""
        identifier = Identifier.create("mydb", "orders", system_table="files")
        self.assertEqual(identifier.object, "orders$files")
        self.assertEqual(identifier.get_table_name(), "orders")
        self.assertIsNone(identifier.get_branch_name())
        self.assertEqual(identifier.get_system_table_name(), "files")

    def test_equality_and_hash_ignore_cached_fields(self):
        """Equality and hash depend only on (database, object), matching Java JSON shape."""
        a = Identifier("mydb", "orders$branch_dev$snapshots")
        b = Identifier.create(
            "mydb", "orders", branch="dev", system_table="snapshots"
        )
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))


class IdentifierBackwardCompatibilityShimTest(unittest.TestCase):
    """Locks in the backward-compat shim for the previous Identifier shape.

    The shim is intentionally narrow: it covers the three concrete API
    breaks that escaped pre-review (constructor ``branch=`` kwarg,
    ``identifier.branch`` attribute, ``Identifier.create(db, object)``
    two-arg form with ``$`` in the second arg). It is scheduled for
    removal in the next minor release; tests below assert both the
    behavioural compatibility AND the ``DeprecationWarning`` so the
    shim doesn't get silently dropped before users have a chance to
    migrate.
    """

    def test_constructor_branch_kwarg_still_works_with_warning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            identifier = Identifier(database="db", object="tbl",
                                    branch="feature")

        depr = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(depr), 1, "exactly one DeprecationWarning expected")
        self.assertIn("Identifier(..., branch=...)", str(depr[0].message))

        # Branch must be encoded into the object field, matching the
        # behaviour of the new ``Identifier.create(..., branch=...)`` form.
        self.assertEqual(identifier.object, "tbl$branch_feature")
        self.assertEqual(identifier.get_branch_name(), "feature")
        self.assertEqual(
            identifier,
            Identifier.create("db", "tbl", branch="feature"),
            "shim must produce a wire-equal Identifier",
        )

    def test_constructor_branch_kwarg_main_is_not_encoded(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            identifier = Identifier("db", "tbl", branch="main")
        # main is the default branch; encoding rule matches Java/create().
        self.assertEqual(identifier.object, "tbl")

    def test_constructor_branch_kwarg_none_is_no_op(self):
        # Explicit branch=None must still trigger the warning (it's the
        # deprecated kwarg form) but produce the un-encoded object.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            identifier = Identifier("db", "tbl", branch=None)
        depr = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(depr), 1)
        self.assertEqual(identifier.object, "tbl")

    def test_branch_property_delegates_to_get_branch_name(self):
        identifier = Identifier.create("db", "tbl", branch="dev")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            value = identifier.branch
        depr = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(depr), 1)
        self.assertIn("Identifier.branch is deprecated", str(depr[0].message))
        self.assertEqual(value, "dev")
        self.assertEqual(value, identifier.get_branch_name())

    def test_branch_property_returns_none_when_no_branch(self):
        identifier = Identifier.create("db", "tbl")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.assertIsNone(identifier.branch)

    def test_create_two_arg_form_with_dollar_falls_back_to_raw(self):
        # ``create("db", "orders$snapshots")`` used to mean "store
        # orders$snapshots as the object". Honour that shape with a
        # warning so existing callers keep working.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            identifier = Identifier.create("db", "orders$snapshots")
        depr = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(depr), 1)
        self.assertIn("Identifier.create(database, object)",
                      str(depr[0].message))
        self.assertEqual(identifier.object, "orders$snapshots")
        self.assertTrue(identifier.is_system_table())
        self.assertEqual(identifier.get_table_name(), "orders")
        self.assertEqual(identifier.get_system_table_name(), "snapshots")

    def test_create_two_arg_form_without_dollar_does_not_warn(self):
        # The new shape — ``create("db", "tbl")`` — must NOT trigger the
        # legacy fallback (no ``$`` in the table name).
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            identifier = Identifier.create("db", "tbl")
        depr = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(depr), 0,
                         "non-legacy create() must not warn")
        self.assertEqual(identifier.object, "tbl")

    def test_create_with_kwargs_skips_legacy_fallback(self):
        # When branch=/system_table= are explicitly passed, the second
        # arg is the table name even if it contains ``$``: do NOT route
        # through the legacy raw-object fallback.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            identifier = Identifier.create("db", "weird$table",
                                           branch="dev")
        depr = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(depr), 0)
        self.assertEqual(identifier.object, "weird$table$branch_dev")
        self.assertEqual(identifier.get_branch_name(), "dev")
        self.assertEqual(identifier.get_table_name(), "weird$table")


if __name__ == '__main__':
    unittest.main()
