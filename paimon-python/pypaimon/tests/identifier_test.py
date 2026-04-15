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

    def test_get_full_name_with_branch(self):
        """get_full_name() includes branch when set."""
        identifier = Identifier(database="mydb", object="mytable", branch="feature")
        self.assertEqual(identifier.get_full_name(), "mydb.mytable.feature")

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
        self.assertTrue(Identifier.create("mydb", "orders$snapshots").is_system_table())

    def test_is_system_table_schemas_suffix(self):
        """object name '<base>$schemas' is a system table."""
        self.assertTrue(Identifier.create("mydb", "orders$schemas").is_system_table())

    def test_is_system_table_files_suffix(self):
        """object name '<base>$files' is a system table."""
        self.assertTrue(Identifier.create("mydb", "orders$files").is_system_table())


if __name__ == '__main__':
    unittest.main()
