"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import logging
from typing import Dict, List, Optional, Any, TypeVar, Generic
from dataclasses import dataclass
from abc import ABC, abstractmethod
from auth import RESTAuthFunction
from response import GetDatabaseResponse

# 类型定义
T = TypeVar('T')


@dataclass
class Identifier:
    """标识符类"""
    database_name: str
    object_name: str


@dataclass
class PagedList(Generic[T]):
    """分页列表"""
    elements: List[T]
    next_page_token: Optional[str] = None


@dataclass
class Pair:
    """键值对"""
    key: str
    value: str


class Options:
    """配置选项类"""

    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        self._config = config_dict or {}

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        return self._config.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """设置配置值"""
        self._config[key] = value

    def to_map(self) -> Dict[str, Any]:
        """转换为字典"""
        return self._config.copy()


class RESTCatalogOptions:
    """REST 目录选项常量"""
    URI = "uri"
    WAREHOUSE = "warehouse"
    TOKEN_PROVIDER = "token.provider"
    DLF_ACCESS_KEY_ID = "dlf.access-key-i"
    DLF_ACCESS_KEY_SECRET = "dlf.access-key-secret"
    PREFIX = 'prefix'


# 异常类
class RESTException(Exception):
    """REST 异常基类"""
    pass


class AlreadyExistsException(RESTException):
    """资源已存在异常"""
    pass


class ForbiddenException(RESTException):
    """权限不足异常"""
    pass


class NoSuchResourceException(RESTException):
    """资源不存在异常"""
    pass


# 请求和响应基类
class RESTRequest(ABC):
    """REST 请求基类"""
    pass


class RESTResponse(ABC):
    """REST 响应基类"""
    pass


class PagedResponse(RESTResponse, Generic[T]):
    """分页响应基类"""

    @abstractmethod
    def data(self) -> Optional[List[T]]:
        """获取数据"""
        pass

    @abstractmethod
    def get_next_page_token(self) -> Optional[str]:
        """获取下一页令牌"""
        pass


# 具体的请求类
@dataclass
class CreateDatabaseRequest(RESTRequest):
    """创建数据库请求"""
    name: str
    properties: Dict[str, str]


@dataclass
class AlterDatabaseRequest(RESTRequest):
    """修改数据库请求"""
    removals: List[str]
    updates: Dict[str, str]


@dataclass
class CreateTableRequest(RESTRequest):
    """创建表请求"""
    identifier: Identifier
    schema: Any  # Schema 对象


@dataclass
class AlterTableRequest(RESTRequest):
    """修改表请求"""
    changes: List[Any]  # SchemaChange 列表


@dataclass
class RenameTableRequest(RESTRequest):
    """重命名表请求"""
    from_table: Identifier
    to_table: Identifier


@dataclass
class CommitTableRequest(RESTRequest):
    """提交表请求"""
    table_uuid: Optional[str]
    snapshot: Any  # Snapshot 对象
    statistics: List[Any]  # PartitionStatistics 列表


@dataclass
class RollbackTableRequest(RESTRequest):
    """回滚表请求"""
    instant: Any  # Instant 对象


@dataclass
class AuthTableQueryRequest(RESTRequest):
    """表查询认证请求"""
    select: Optional[List[str]]


@dataclass
class MarkDonePartitionsRequest(RESTRequest):
    """标记完成分区请求"""
    partitions: List[Dict[str, str]]


@dataclass
class CreateBranchRequest(RESTRequest):
    """创建分支请求"""
    branch: str
    from_tag: Optional[str]


@dataclass
class ForwardBranchRequest(RESTRequest):
    """前进分支请求"""
    pass


@dataclass
class CreateFunctionRequest(RESTRequest):
    """创建函数请求"""
    function: Any  # Function 对象


@dataclass
class AlterFunctionRequest(RESTRequest):
    """修改函数请求"""
    changes: List[Any]  # FunctionChange 列表


@dataclass
class CreateViewRequest(RESTRequest):
    """创建视图请求"""
    identifier: Identifier
    schema: Any  # ViewSchema 对象


@dataclass
class AlterViewRequest(RESTRequest):
    """修改视图请求"""
    view_changes: List[Any]  # ViewChange 列表


# 具体的响应类
@dataclass
class ConfigResponse(RESTResponse):
    """配置响应"""
    config: Dict[str, Any]

    def merge(self, options: Dict[str, Any]) -> Dict[str, Any]:
        """合并配置"""
        merged = options.copy()
        merged.update(self.config)
        return merged


@dataclass
class ListDatabasesResponse(PagedResponse[str]):
    """列出数据库响应"""
    databases: Optional[List[str]]
    next_page_token: Optional[str]

    def data(self) -> Optional[List[str]]:
        return self.databases

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token



@dataclass
class AlterDatabaseResponse(RESTResponse):
    """修改数据库响应"""
    success: bool


@dataclass
class ListTablesResponse(PagedResponse[str]):
    """列出表响应"""
    tables: Optional[List[str]]
    next_page_token: Optional[str]

    def data(self) -> Optional[List[str]]:
        return self.tables

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class GetTableResponse(RESTResponse):
    """获取表响应"""
    identifier: Identifier
    schema: Any  # Schema 对象
    properties: Dict[str, str]


@dataclass
class ListTableDetailsResponse(PagedResponse):
    """列出表详情响应"""
    table_details: Optional[List[GetTableResponse]]
    next_page_token: Optional[str]

    def data(self) -> Optional[List[GetTableResponse]]:
        return self.table_details

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class ListTablesGloballyResponse(PagedResponse[Identifier]):
    """全局列出表响应"""
    tables: Optional[List[Identifier]]
    next_page_token: Optional[str]

    def data(self) -> Optional[List[Identifier]]:
        return self.tables

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class GetTableSnapshotResponse(RESTResponse):
    """获取表快照响应"""
    snapshot: Any  # TableSnapshot 对象


@dataclass
class GetVersionSnapshotResponse(RESTResponse):
    """获取版本快照响应"""
    snapshot: Any  # Snapshot 对象


@dataclass
class ListSnapshotsResponse(PagedResponse):
    """列出快照响应"""
    snapshots: Optional[List[Any]]  # Snapshot 列表
    next_page_token: Optional[str]

    def data(self) -> Optional[List[Any]]:
        return self.snapshots

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class CommitTableResponse(RESTResponse):
    """提交表响应"""
    success: bool

    def is_success(self) -> bool:
        return self.success


@dataclass
class AuthTableQueryResponse(RESTResponse):
    """表查询认证响应"""
    filter_conditions: List[str]

    def filter(self) -> List[str]:
        return self.filter_conditions


@dataclass
class ListPartitionsResponse(PagedResponse):
    """列出分区响应"""
    partitions: Optional[List[Any]]  # Partition 列表
    next_page_token: Optional[str]

    def data(self) -> Optional[List[Any]]:
        return self.partitions

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class ListBranchesResponse(RESTResponse):
    """列出分支响应"""
    branch_list: Optional[List[str]]

    def branches(self) -> Optional[List[str]]:
        return self.branch_list


@dataclass
class ListFunctionsResponse(PagedResponse[str]):
    """列出函数响应"""
    function_list: Optional[List[str]]
    next_page_token: Optional[str]

    def functions(self) -> Optional[List[str]]:
        return self.function_list

    def data(self) -> Optional[List[str]]:
        return self.function_list

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class GetFunctionResponse(RESTResponse):
    """获取函数响应"""
    identifier: Identifier
    function: Any  # Function 对象


@dataclass
class ListFunctionDetailsResponse(PagedResponse):
    """列出函数详情响应"""
    function_details: Optional[List[GetFunctionResponse]]
    next_page_token: Optional[str]

    def data(self) -> Optional[List[GetFunctionResponse]]:
        return self.function_details

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class ListFunctionsGloballyResponse(PagedResponse[Identifier]):
    """全局列出函数响应"""
    functions: Optional[List[Identifier]]
    next_page_token: Optional[str]

    def data(self) -> Optional[List[Identifier]]:
        return self.functions

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class ListViewsResponse(PagedResponse[str]):
    """列出视图响应"""
    views: Optional[List[str]]
    next_page_token: Optional[str]

    def get_views(self) -> Optional[List[str]]:
        return self.views

    def data(self) -> Optional[List[str]]:
        return self.views

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class GetViewResponse(RESTResponse):
    """获取视图响应"""
    identifier: Identifier
    schema: Any  # ViewSchema 对象


@dataclass
class ListViewDetailsResponse(PagedResponse):
    """列出视图详情响应"""
    view_details: Optional[List[GetViewResponse]]
    next_page_token: Optional[str]

    def get_view_details(self) -> Optional[List[GetViewResponse]]:
        return self.view_details

    def data(self) -> Optional[List[GetViewResponse]]:
        return self.view_details

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class ListViewsGloballyResponse(PagedResponse[Identifier]):
    """全局列出视图响应"""
    views: Optional[List[Identifier]]
    next_page_token: Optional[str]

    def get_views(self) -> Optional[List[Identifier]]:
        return self.views

    def data(self) -> Optional[List[Identifier]]:
        return self.views

    def get_next_page_token(self) -> Optional[str]:
        return self.next_page_token


@dataclass
class GetTableTokenResponse(RESTResponse):
    """获取表令牌响应"""
    token: str
    expires_at: int


# 工具类
class RESTUtil:
    """REST 工具类"""

    @staticmethod
    def encode_string(value: str) -> str:
        """编码字符串"""
        import urllib.parse
        return urllib.parse.quote(value)

    @staticmethod
    def extract_prefix_map(options: Dict[str, str], prefix: str) -> Dict[str, str]:
        """提取前缀映射"""
        result = {}
        config = options
        for key, value in config.items():
            if key.startswith(prefix):
                new_key = key[len(prefix):]
                result[new_key] = str(value)
        return result


class ResourcePaths:
    """资源路径类"""

    def __init__(self, base_path: str = ""):
        self.base_path = base_path.rstrip('/')

    @classmethod
    def for_catalog_properties(cls, options: Options) -> 'ResourcePaths':
        """根据目录属性创建资源路径"""
        prefix = options.get(RESTCatalogOptions.PREFIX, "")
        return cls(f"/v1/{prefix}" if prefix else "/v1")

    def config(self) -> str:
        """配置路径"""
        return "/v1/config"

    def databases(self) -> str:
        """数据库列表路径"""
        return f"{self.base_path}/databases"

    def database(self, name: str) -> str:
        """单个数据库路径"""
        return f"{self.base_path}/databases/{name}"

    def tables(self, database_name: Optional[str] = None) -> str:
        """表列表路径"""
        if database_name:
            return f"{self.base_path}/databases/{database_name}/tables"
        return f"{self.base_path}/tables"

    def table(self, database_name: str, table_name: str) -> str:
        """单个表路径"""
        return f"{self.base_path}/databases/{database_name}/tables/{table_name}"

    def table_details(self, database_name: str) -> str:
        """表详情路径"""
        return f"{self.base_path}/databases/{database_name}/table-details"

    def table_snapshot(self, database_name: str, table_name: str, version: Optional[str] = None) -> str:
        """表快照路径"""
        base = f"{self.base_path}/databases/{database_name}/tables/{table_name}/snapshot"
        return f"{base}/{version}" if version else base

    def snapshots(self, database_name: str, table_name: str) -> str:
        """快照列表路径"""
        return f"{self.base_path}/databases/{database_name}/tables/{table_name}/snapshots"

    def commit_table(self, database_name: str, table_name: str) -> str:
        """提交表路径"""
        return f"{self.base_path}/databases/{database_name}/tables/{table_name}/commit"

    def rollback_table(self, database_name: str, table_name: str) -> str:
        """回滚表路径"""
        return f"{self.base_path}/databases/{database_name}/tables/{table_name}/rollback"

    def rename_table(self) -> str:
        """重命名表路径"""
        return f"{self.base_path}/rename-table"

    def auth_table(self, database_name: str, table_name: str) -> str:
        """表认证路径"""
        return f"{self.base_path}/databases/{database_name}/tables/{table_name}/auth"

    def mark_done_partitions(self, database_name: str, table_name: str) -> str:
        """标记完成分区路径"""
        return f"{self.base_path}/databases/{database_name}/tables/{table_name}/mark-done-partitions"

    def partitions(self, database_name: str, table_name: str) -> str:
        """分区列表路径"""
        return f"{self.base_path}/databases/{database_name}/tables/{table_name}/partitions"

    def branches(self, database_name: str, table_name: str) -> str:
        """分支列表路径"""
        return f"{self.base_path}/databases/{database_name}/tables/{table_name}/branches"

    def branch(self, database_name: str, table_name: str, branch_name: str) -> str:
        """单个分支路径"""
        return f"{self.base_path}/databases/{database_name}/tables/{table_name}/branches/{branch_name}"

    def forward_branch(self, database_name: str, table_name: str, branch_name: str) -> str:
        """前进分支路径"""
        return f"{self.base_path}/databases/{database_name}/tables/{table_name}/branches/{branch_name}/forward"

    def functions(self, database_name: Optional[str] = None) -> str:
        """函数列表路径"""
        if database_name:
            return f"{self.base_path}/databases/{database_name}/functions"
        return f"{self.base_path}/functions"

    def function(self, database_name: str, function_name: str) -> str:
        """单个函数路径"""
        return f"{self.base_path}/databases/{database_name}/functions/{function_name}"

    def function_details(self, database_name: str) -> str:
        """函数详情路径"""
        return f"{self.base_path}/databases/{database_name}/function-details"

    def views(self, database_name: Optional[str] = None) -> str:
        """视图列表路径"""
        if database_name:
            return f"{self.base_path}/databases/{database_name}/views"
        return f"{self.base_path}/views"

    def view(self, database_name: str, view_name: str) -> str:
        """单个视图路径"""
        return f"{self.base_path}/databases/{database_name}/views/{view_name}"

    def view_details(self, database_name: str) -> str:
        """视图详情路径"""
        return f"{self.base_path}/databases/{database_name}/view-details"

    def rename_view(self) -> str:
        """重命名视图路径"""
        return f"{self.base_path}/rename-view"

    def table_token(self, database_name: str, table_name: str) -> str:
        """表令牌路径"""
        return f"{self.base_path}/databases/{database_name}/tables/{table_name}/token"


class RESTFunctionValidator:
    """REST 函数验证器"""

    @staticmethod
    def is_valid_function_name(name: str) -> bool:
        """验证函数名是否有效"""
        if not name or not name.strip():
            return False
        # 简单的验证逻辑，可以根据需要扩展
        return name.replace('_', '').replace('-', '').isalnum()

    @staticmethod
    def check_function_name(name: str) -> None:
        """检查函数名，无效时抛出异常"""
        if not RESTFunctionValidator.is_valid_function_name(name):
            raise ValueError(f"Invalid function name: {name}")


# 主要的 REST API 类
class RESTApi:
    """
    REST API for REST Catalog.

    This API class only includes interaction with REST Server and does not have file read and
    write operations, which makes this API lightweight enough to avoid introducing dependencies such
    as Hadoop and file systems.

    Example:
        options = Options({
            RESTCatalogOptions.URI: "<rest server url>",
            RESTCatalogOptions.WAREHOUSE: "my_instance_name",
            RESTCatalogOptions.TOKEN_PROVIDER: "dlf",
            RESTCatalogOptions.DLF_ACCESS_KEY_ID: "<access-key-id>",
            RESTCatalogOptions.DLF_ACCESS_KEY_SECRET: "<access-key-secret>"
        })

        api = RESTApi(options)
        tables = api.list_tables("my_database")
        print(tables)
    """

    # 常量定义
    HEADER_PREFIX = "header."
    MAX_RESULTS = "maxResults"
    PAGE_TOKEN = "pageToken"

    DATABASE_NAME_PATTERN = "databaseNamePattern"
    TABLE_NAME_PATTERN = "tableNamePattern"
    VIEW_NAME_PATTERN = "viewNamePattern"
    FUNCTION_NAME_PATTERN = "functionNamePattern"
    PARTITION_NAME_PATTERN = "partitionNamePattern"

    TOKEN_EXPIRATION_SAFE_TIME_MILLIS = 3_600_000  # 1小时

    def __init__(self, options: Dict[str, str], config_required: bool = True):
        """
        初始化 REST API

        Args:
            options: 包含认证和目录信息的配置选项
            config_required: 是否需要在初始化时合并配置
        """
        self.logger = logging.getLogger(self.__class__.__name__)

        # 导入必要的模块（假设已实现）
        from client import HttpClient
        from auth import DLFAuthProvider, DLFToken

        self.client = HttpClient(options.get(RESTCatalogOptions.URI))
        auth_provider = DLFAuthProvider(DLFToken(access_key_id=options.get("dlf.access-key-id"), access_key_secret=options.get("dlf.access-key-secret")))
        base_headers = RESTUtil.extract_prefix_map(options, self.HEADER_PREFIX)

        if config_required:
            warehouse = options.get(RESTCatalogOptions.WAREHOUSE)
            query_params = {}
            if warehouse:
                query_params[RESTCatalogOptions.WAREHOUSE] = RESTUtil.encode_string(warehouse)

            config_response = self.client.get_with_params(
                ResourcePaths().config(),
                query_params,
                ConfigResponse,
                RESTAuthFunction({}, auth_provider)
            )
            options.update(config_response.get('defaults'))
            base_headers.update(RESTUtil.extract_prefix_map(options, self.HEADER_PREFIX))

        self.rest_auth_function = RESTAuthFunction(base_headers, auth_provider)
        self.options = options
        self.resource_paths = ResourcePaths.for_catalog_properties(options)

    def get_options(self) -> Options:
        """获取已从 REST 服务器合并的配置选项"""
        return self.options

    # 数据库操作
    def list_databases(self) -> List[str]:
        """列出数据库"""
        return self._list_data_from_page_api(
            lambda query_params: self.client.get_with_params(
                self.resource_paths.databases(),
                query_params,
                ListDatabasesResponse,
                self.rest_auth_function
            )
        )

    def list_databases_paged(self,
                             max_results: Optional[int] = None,
                             page_token: Optional[str] = None,
                             database_name_pattern: Optional[str] = None) -> PagedList[str]:
        """分页列出数据库"""
        response = self.client.get_with_params(
            self.resource_paths.databases(),
            self._build_paged_query_params(
                max_results,
                page_token,
                Pair(self.DATABASE_NAME_PATTERN, database_name_pattern)
            ),
            ListDatabasesResponse,
            self.rest_auth_function
        )

        databases = response.get_databases() or []
        return PagedList(databases, response.get_next_page_token())

    def create_database(self, name: str, properties: Dict[str, str]) -> None:
        """创建数据库"""
        request = CreateDatabaseRequest(name, properties)
        self.client.post(self.resource_paths.databases(), request, self.rest_auth_function)

    def get_database(self, name: str) -> GetDatabaseResponse:
        """获取数据库"""
        return self.client.get(
            self.resource_paths.database(name),
            GetDatabaseResponse,
            self.rest_auth_function
        )

    def drop_database(self, name: str) -> None:
        """删除数据库"""
        self.client.delete(self.resource_paths.database(name), self.rest_auth_function)

    def alter_database(self, name: str, removals: List[str], updates: Dict[str, str]) -> None:
        """修改数据库"""
        self.client.post_with_response_type(
            self.resource_paths.database(name),
            AlterDatabaseRequest(removals, updates),
            AlterDatabaseResponse,
            self.rest_auth_function
        )

    # 表操作
    def list_tables(self, database_name: str) -> List[str]:
        """列出表"""
        return self._list_data_from_page_api(
            lambda query_params: self.client.get_with_params(
                self.resource_paths.tables(database_name),
                query_params,
                ListTablesResponse,
                self.rest_auth_function
            )
        )

    def list_tables_paged(self,
                          database_name: str,
                          max_results: Optional[int] = None,
                          page_token: Optional[str] = None,
                          table_name_pattern: Optional[str] = None) -> PagedList[str]:
        """分页列出表"""
        response = self.client.get_with_params(
            self.resource_paths.tables(database_name),
            self._build_paged_query_params(
                max_results,
                page_token,
                Pair(self.TABLE_NAME_PATTERN, table_name_pattern)
            ),
            ListTablesResponse,
            self.rest_auth_function
        )

        tables = response.get_tables() or []
        return PagedList(tables, response.get_next_page_token())

    def get_table(self, identifier: Identifier) -> GetTableResponse:
        """获取表"""
        return self.client.get(
            self.resource_paths.table(identifier.database_name, identifier.object_name),
            GetTableResponse,
            self.rest_auth_function
        )

    def create_table(self, identifier: Identifier, schema: Any) -> None:
        """创建表"""
        request = CreateTableRequest(identifier, schema)
        self.client.post(
            self.resource_paths.tables(identifier.database_name),
            request,
            self.rest_auth_function
        )

    def drop_table(self, identifier: Identifier) -> None:
        """删除表"""
        self.client.delete(
            self.resource_paths.table(identifier.database_name, identifier.object_name),
            self.rest_auth_function
        )

    def alter_table(self, identifier: Identifier, changes: List[Any]) -> None:
        """修改表"""
        request = AlterTableRequest(changes)
        self.client.post_with_response_type(
            self.resource_paths.table(identifier.database_name, identifier.object_name),
            request,
            None,
            self.rest_auth_function
        )

    def rename_table(self, from_table: Identifier, to_table: Identifier) -> None:
        """重命名表"""
        request = RenameTableRequest(from_table, to_table)
        self.client.post(self.resource_paths.rename_table(), request, self.rest_auth_function)

    # 快照操作
    def load_snapshot(self, identifier: Identifier, version: Optional[str] = None) -> Any:
        """加载快照"""
        if version:
            response = self.client.get(
                self.resource_paths.table_snapshot(
                    identifier.database_name, identifier.object_name, version
                ),
                GetVersionSnapshotResponse,
                self.rest_auth_function
            )
            return response.snapshot
        else:
            response = self.client.get(
                self.resource_paths.table_snapshot(
                    identifier.database_name, identifier.object_name
                ),
                GetTableSnapshotResponse,
                self.rest_auth_function
            )
            return response.snapshot

    def commit_snapshot(self,
                        identifier: Identifier,
                        table_uuid: Optional[str],
                        snapshot: Any,
                        statistics: List[Any]) -> bool:
        """提交快照"""
        request = CommitTableRequest(table_uuid, snapshot, statistics)
        response = self.client.post_with_response_type(
            self.resource_paths.commit_table(identifier.database_name, identifier.object_name),
            request,
            CommitTableResponse,
            self.rest_auth_function
        )
        return response.is_success()

    def rollback_to(self, identifier: Identifier, instant: Any) -> None:
        request = RollbackTableRequest(instant)
        self.client.post(
            self.resource_paths.rollback_table(identifier.database_name, identifier.object_name),
            request,
            self.rest_auth_function
        )

    # 分区操作
    def list_partitions(self, identifier: Identifier) -> List[Any]:
        return self._list_data_from_page_api(
            lambda query_params: self.client.get_with_params(
                self.resource_paths.partitions(identifier.database_name, identifier.object_name),
                query_params,
                ListPartitionsResponse,
                self.rest_auth_function
            )
        )

    def mark_done_partitions(self, identifier: Identifier, partitions: List[Dict[str, str]]) -> None:
        request = MarkDonePartitionsRequest(partitions)
        self.client.post(
            self.resource_paths.mark_done_partitions(
                identifier.database_name, identifier.object_name
            ),
            request,
            self.rest_auth_function
        )

    # 分支操作
    def create_branch(self, identifier: Identifier, branch: str, from_tag: Optional[str] = None) -> None:
        request = CreateBranchRequest(branch, from_tag)
        self.client.post(
            self.resource_paths.branches(identifier.database_name, identifier.object_name),
            request,
            self.rest_auth_function
        )

    def drop_branch(self, identifier: Identifier, branch: str) -> None:
        self.client.delete(
            self.resource_paths.branch(identifier.database_name, identifier.object_name, branch),
            self.rest_auth_function
        )

    def list_branches(self, identifier: Identifier) -> List[str]:
        response = self.client.get(
            self.resource_paths.branches(identifier.database_name, identifier.object_name),
            ListBranchesResponse,
            self.rest_auth_function
        )
        return response.branches() or []

if __name__ == "__main__":
    options = {}
    api = RESTApi(options)
    response = api.get_database("default")
    print(response)
