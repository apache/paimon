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
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow as pa

from pypaimon.api.rest_api import RESTApi
from pypaimon.common.identifier import Identifier
from pypaimon.function.function import FunctionImpl
from pypaimon.function.function_change import FunctionChange
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.schema.data_types import AtomicType
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.table.instant import SnapshotInstant

DB = "/v1/catalog/databases/db"
TABLE = DB + "/tables/t"
ID = Identifier.create("db", "t")
SNAPSHOT = Snapshot(version=3, id=1, schema_id=0, base_manifest_list="base", delta_manifest_list="delta",
                    total_record_count=1, delta_record_count=1,
                    commit_user="u", commit_identifier=1, commit_kind="APPEND", time_millis=0)


class _Recorder(BaseHTTPRequestHandler):
    requests = []

    def _record(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)
        _Recorder.requests.append(
            (self.command, self.path.split("?")[0], self.headers.get("x-acs-action")))
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = do_DELETE = _record

    def log_message(self, *args):
        pass


class RESTApiNameTest(unittest.TestCase):
    """Every RESTApi call names its API, and the ACS4 signer sends it as x-acs-action."""

    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Recorder)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.options = {
            "uri": "http://127.0.0.1:%d" % self.server.server_address[1],
            "prefix": "catalog",
            "token.provider": "dlf",
            "dlf.access-key-id": "akId",
            "dlf.access-key-secret": "akSecret",
            "dlf.region": "cn-hangzhou",
            "dlf.signing-algorithm": "openapi-v4",
        }
        self.api = RESTApi(dict(self.options), config_required=False)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def expect(self, method, path, api_name, call):
        """Stub replies do not parse into every response type, so only the outgoing requests count."""
        _Recorder.requests.clear()
        try:
            call()
        except Exception:
            pass
        self.assertTrue(_Recorder.requests, "%s %s sent nothing" % (method, path))
        for request in _Recorder.requests:
            self.assertEqual((method, path, api_name), request)

    def test_config(self):
        options = dict(self.options, warehouse="wh")
        self.expect("GET", "/v1/config", "GetConfig", lambda: RESTApi(options))

    def test_databases(self):
        dbs = "/v1/catalog/databases"
        api = self.api
        self.expect("GET", dbs, "ListDatabases", api.list_databases)
        self.expect("GET", dbs, "ListDatabases", lambda: api.list_databases_paged(10, None, "d%"))
        self.expect("POST", dbs, "CreateDatabase", lambda: api.create_database("db", {}))
        self.expect("GET", DB, "GetDatabase", lambda: api.get_database("db"))
        self.expect("POST", DB, "AlterDatabase", lambda: api.alter_database("db", updates={"k": "v"}))
        self.expect("DELETE", DB, "DropDatabase", lambda: api.drop_database("db"))

    def test_tables(self):
        api = self.api
        schema = Schema.from_pyarrow_schema(pa.schema([("id", pa.int32())]))
        self.expect("GET", DB + "/tables", "ListTables", lambda: api.list_tables("db"))
        self.expect("GET", DB + "/tables", "ListTables", lambda: api.list_tables_paged("db", 10))
        self.expect("POST", DB + "/tables", "CreateTable", lambda: api.create_table(ID, schema))
        self.expect("GET", TABLE, "GetTable", lambda: api.get_table(ID))
        self.expect("POST", TABLE, "AlterTable",
                    lambda: api.alter_table(ID, [SchemaChange.add_column("c", AtomicType("INT"))]))
        self.expect("DELETE", TABLE, "DropTable", lambda: api.drop_table(ID))
        self.expect("POST", "/v1/catalog/tables/rename", "RenameTable",
                    lambda: api.rename_table(ID, Identifier.create("db", "t2")))
        self.expect("GET", TABLE + "/token", "GetTableToken", lambda: api.load_table_token(ID))
        self.expect("POST", TABLE + "/auth", "AuthTableQuery", lambda: api.auth_table_query(ID, ["id"]))

    def test_snapshots_and_partitions(self):
        api = self.api
        self.expect("POST", TABLE + "/commit", "CommitTable",
                    lambda: api.commit_snapshot(ID, "uuid", None, SNAPSHOT, []))
        self.expect("GET", TABLE + "/snapshot", "GetTableSnapshot", lambda: api.load_snapshot(ID))
        self.expect("POST", TABLE + "/rollback", "RollbackToSnapshot",
                    lambda: api.rollback_to(ID, SnapshotInstant(1)))
        self.expect("GET", TABLE + "/partitions", "ListPartitions",
                    lambda: api.list_partitions_paged(ID, 10))
        self.expect("POST", TABLE + "/partitions", "CreatePartitions",
                    lambda: api.create_partitions(ID, [{"dt": "1"}]))

    def test_branches_and_tags(self):
        api = self.api
        self.expect("GET", TABLE + "/branches", "ListBranches", lambda: api.list_branches(ID))
        self.expect("POST", TABLE + "/branches", "CreateBranch", lambda: api.create_branch(ID, "b"))
        self.expect("DELETE", TABLE + "/branches/b", "DropBranch", lambda: api.drop_branch(ID, "b"))
        self.expect("POST", TABLE + "/branches/b/forward", "FastForwardBranch",
                    lambda: api.fast_forward(ID, "b"))
        self.expect("POST", TABLE + "/branches/b/rename", "RenameBranch",
                    lambda: api.rename_branch(ID, "b", "b2"))
        self.expect("GET", TABLE + "/tags", "ListTags", lambda: api.list_tags_paged(ID, 10))
        self.expect("POST", TABLE + "/tags", "CreateTag", lambda: api.create_tag(ID, "tag", 1))
        self.expect("GET", TABLE + "/tags/tag", "GetTag", lambda: api.get_tag(ID, "tag"))
        self.expect("DELETE", TABLE + "/tags/tag", "DropTag", lambda: api.delete_tag(ID, "tag"))

    def test_functions(self):
        api = self.api
        function = Identifier.create("db", "f")
        self.expect("GET", DB + "/functions", "ListFunctions", lambda: api.list_functions("db"))
        self.expect("GET", DB + "/functions", "ListFunctions",
                    lambda: api.list_functions_paged("db", 10))
        self.expect("GET", DB + "/function-details", "ListFunctionDetails",
                    lambda: api.list_function_details_paged("db", 10))
        self.expect("GET", "/v1/catalog/functions", "ListFunctionsGlobally",
                    lambda: api.list_functions_paged_globally(max_results=10))
        self.expect("POST", DB + "/functions", "CreateFunction",
                    lambda: api.create_function(function, FunctionImpl(function)))
        self.expect("GET", DB + "/functions/f", "GetFunction", lambda: api.get_function(function))
        self.expect("POST", DB + "/functions/f", "AlterFunction",
                    lambda: api.alter_function(function, [FunctionChange.set_option("k", "v")]))
        self.expect("DELETE", DB + "/functions/f", "DropFunction", lambda: api.drop_function(function))


if __name__ == '__main__':
    unittest.main()
