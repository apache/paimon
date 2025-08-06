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

from pypaimon.common.identifier import Identifier


# Exception classes
class CatalogException(Exception):
    """Base catalog exception"""


class DatabaseNotExistException(CatalogException):
    """Database not exist exception"""

    def __init__(self, database: str):
        self.database = database
        super().__init__(f"Database {database} does not exist")


class DatabaseAlreadyExistException(CatalogException):
    """Database already exist exception"""

    def __init__(self, database: str):
        self.database = database
        super().__init__(f"Database {database} already exists")


class DatabaseNoPermissionException(CatalogException):
    """Database no permission exception"""

    def __init__(self, database: str):
        self.database = database
        super().__init__(f"No permission to access database {database}")


class TableNotExistException(CatalogException):
    """Table not exist exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"Table {identifier.get_full_name()} does not exist")


class TableAlreadyExistException(CatalogException):
    """Table already exist exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"Table {identifier.get_full_name()} already exists")


class TableNoPermissionException(CatalogException):
    """Table no permission exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"No permission to access table {identifier.get_full_name()}")


class ViewNotExistException(CatalogException):
    """View not exist exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"View {identifier.get_full_name()} does not exist")


class ViewAlreadyExistException(CatalogException):
    """View already exist exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"View {identifier.get_full_name()} already exists")


class FunctionNotExistException(CatalogException):
    """Function not exist exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"Function {identifier.get_full_name()} does not exist")


class FunctionAlreadyExistException(CatalogException):
    """Function already exist exception"""

    def __init__(self, identifier: Identifier):
        self.identifier = identifier
        super().__init__(f"Function {identifier.get_full_name()} already exists")


class ColumnNotExistException(CatalogException):
    """Column not exist exception"""

    def __init__(self, column: str):
        self.column = column
        super().__init__(f"Column {column} does not exist")


class ColumnAlreadyExistException(CatalogException):
    """Column already exist exception"""

    def __init__(self, column: str):
        self.column = column
        super().__init__(f"Column {column} already exists")


class DefinitionNotExistException(CatalogException):
    """Definition not exist exception"""

    def __init__(self, identifier: Identifier, name: str):
        self.identifier = identifier
        self.name = name
        super().__init__(f"Definition {name} does not exist in {identifier.get_full_name()}")


class DefinitionAlreadyExistException(CatalogException):
    """Definition already exist exception"""

    def __init__(self, identifier: Identifier, name: str):
        self.identifier = identifier
        self.name = name
        super().__init__(f"Definition {name} already exists in {identifier.get_full_name()}")


class DialectNotExistException(CatalogException):
    """Dialect not exist exception"""

    def __init__(self, identifier: Identifier, dialect: str):
        self.identifier = identifier
        self.dialect = dialect
        super().__init__(f"Dialect {dialect} does not exist in {identifier.get_full_name()}")


class DialectAlreadyExistException(CatalogException):
    """Dialect already exist exception"""

    def __init__(self, identifier: Identifier, dialect: str):
        self.identifier = identifier
        self.dialect = dialect
        super().__init__(f"Dialect {dialect} already exists in {identifier.get_full_name()}")
