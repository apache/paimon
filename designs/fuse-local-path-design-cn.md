<!--
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
-->

# RESTCatalog FUSE 本地路径配置设计

## 背景

在使用 Paimon RESTCatalog 访问远端对象存储（如 OSS、S3、HDFS）时，数据访问通常通过远端存储 SDK 进行。然而，在远端存储路径通过 FUSE（用户空间文件系统）挂载到本地的场景下，用户可以直接通过本地文件系统路径访问数据，获得更好的性能。

本设计引入配置参数以支持 FUSE 挂载的远端存储路径，允许用户在 Catalog、Database 和 Table 三个层级指定本地路径映射。

## 目标

1. 支持通过本地文件系统访问 FUSE 挂载的远端存储路径
2. 支持分层路径映射：Catalog 根路径 > Database > Table
3. 当 FUSE 本地路径适用时，使用本地 FileIO 进行数据读写
4. 保持与现有 RESTCatalog 行为的向后兼容性

---

## 【高】需求一：增加 FUSE 相关配置

### 配置参数

所有参数定义在 `pypaimon/common/options/config.py` 中的 `FuseOptions` 类：

| 参数 | 类型 | 默认值 | 描述 |
|-----|------|--------|------|
| `fuse.local-path.enabled` | Boolean | `false` | 是否启用 FUSE 本地路径映射 |
| `fuse.local-path.root` | String | (无) | FUSE 挂载的本地根路径，如 `/mnt/fuse` |
| `fuse.local-path.database` | String | (无) | Database 级别的本地路径映射。格式：`db1:/local/path1,db2:/local/path2` |
| `fuse.local-path.table` | String | (无) | Table 级别的本地路径映射。格式：`db1.table1:/local/path1,db2.table2:/local/path2` |

### 使用示例

```python
from pypaimon import Catalog

# 创建 REST Catalog 并启用 FUSE 本地路径
catalog = Catalog.create({
    'metastore': 'rest',
    'uri': 'http://rest-server:8080',
    'token': 'xxx',

    # FUSE 本地路径配置
    'fuse.local-path.enabled': 'true',
    'fuse.local-path.root': '/mnt/fuse/warehouse',
    'fuse.local-path.database': 'db1:/mnt/custom/db1,db2:/mnt/custom/db2',
    'fuse.local-path.table': 'db1.table1:/mnt/special/t1'
})
```

### 路径解析优先级

解析路径时，系统按以下顺序检查（优先级从高到低）：

1. **Table 级别映射**（`fuse.local-path.table`）
2. **Database 级别映射**（`fuse.local-path.database`）
3. **根路径映射**（`fuse.local-path.root`）

示例：对于表 `db1.table1`：
- 如果 `fuse.local-path.table` 包含 `db1.table1:/mnt/special/t1`，使用 `/mnt/special/t1`
- 否则，如果 `fuse.local-path.database` 包含 `db1:/mnt/custom/db1`，使用 `/mnt/custom/db1`
- 否则，使用 `fuse.local-path.root`（如 `/mnt/fuse/warehouse`）

### 行为矩阵

| 配置 | 路径匹配 | 行为 |
|-----|---------|------|
| `fuse.local-path.enabled=true` | 是 | 本地 FileIO 进行数据读写 |
| `fuse.local-path.enabled=true` | 否 | 回退到原有逻辑 |
| `fuse.local-path.enabled=false` | 不适用 | 原有逻辑（data token 或 FileIO.get） |

### FuseOptions 配置类定义

在 `pypaimon/common/options/config.py` 中添加：

```python
class FuseOptions:
    """FUSE 本地路径配置选项。"""
    
    FUSE_LOCAL_PATH_ENABLED = (
        ConfigOptions.key("fuse.local-path.enabled")
        .boolean_type()
        .default_value(False)
        .with_description("是否启用 FUSE 本地路径映射")
    )
    
    FUSE_LOCAL_PATH_ROOT = (
        ConfigOptions.key("fuse.local-path.root")
        .string_type()
        .no_default_value()
        .with_description("FUSE 挂载的本地根路径，如 /mnt/fuse")
    )
    
    FUSE_LOCAL_PATH_DATABASE = (
        ConfigOptions.key("fuse.local-path.database")
        .string_type()
        .no_default_value()
        .with_description(
            "Database 级别的本地路径映射。格式：db1:/local/path1,db2:/local/path2"
        )
    )
    
    FUSE_LOCAL_PATH_TABLE = (
        ConfigOptions.key("fuse.local-path.table")
        .string_type()
        .no_default_value()
        .with_description(
            "Table 级别的本地路径映射。格式：db1.table1:/local/path1,db2.table2:/local/path2"
        )
    )
```

### RESTCatalog 修改

修改 `pypaimon/catalog/rest/rest_catalog.py` 中的 `file_io_for_data` 方法：

```python
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.common.options.config import FuseOptions

class RESTCatalog(Catalog):
    def __init__(self, context: CatalogContext, config_required: Optional[bool] = True):
        # ... 原有初始化代码 ...
        self.fuse_local_path_enabled = self.context.options.get(
            FuseOptions.FUSE_LOCAL_PATH_ENABLED, False)

    def file_io_for_data(self, table_path: str, identifier: Identifier) -> FileIO:
        """获取用于数据访问的 FileIO，支持 FUSE 本地路径映射。"""
        # 如果 FUSE 本地路径启用且路径匹配，使用本地 FileIO
        if self.fuse_local_path_enabled:
            local_path = self._resolve_fuse_local_path(table_path, identifier)
            if local_path is not None:
                # 使用本地文件 IO，无需 token
                return LocalFileIO(local_path, self.context.options)

        # 原有逻辑：data token 或 FileIO.get
        return RESTTokenFileIO(identifier, table_path, self.context.options) \
            if self.data_token_enabled else self.file_io_from_options(table_path)

    def _resolve_fuse_local_path(self, original_path: str, identifier: Identifier) -> Optional[str]:
        """
        解析 FUSE 本地路径。优先级：table > database > root。
        
        Returns:
            本地路径，如果不适用则返回 None
        """
        # 1. 检查 Table 级别映射
        table_mappings = self._parse_map_option(
            self.context.options.get(FuseOptions.FUSE_LOCAL_PATH_TABLE))
        table_key = f"{identifier.get_database_name()}.{identifier.get_table_name()}"
        if table_key in table_mappings:
            return self._convert_to_local_path(original_path, table_mappings[table_key])

        # 2. 检查 Database 级别映射
        db_mappings = self._parse_map_option(
            self.context.options.get(FuseOptions.FUSE_LOCAL_PATH_DATABASE))
        if identifier.get_database_name() in db_mappings:
            return self._convert_to_local_path(
                original_path, db_mappings[identifier.get_database_name()])

        # 3. 使用根路径映射
        fuse_root = self.context.options.get(FuseOptions.FUSE_LOCAL_PATH_ROOT)
        if fuse_root:
            return self._convert_to_local_path(original_path, fuse_root)

        return None

    def _convert_to_local_path(self, original_path: str, local_root: str) -> str:
        """将远端存储路径转换为本地 FUSE 路径。
        
        示例：oss://bucket/warehouse/db1/table1 -> /mnt/fuse/warehouse/db1/table1
        """
        from urllib.parse import urlparse
        
        uri = urlparse(original_path)
        if not uri.scheme:
            # 已经是本地路径
            return original_path
            
        # 提取路径部分，移除开头的 scheme 和 netloc
        path_part = uri.path
        
        # 确保路径不以 / 开头（local_root 已经是完整路径）
        if path_part.startswith('/'):
            path_part = path_part[1:]
            
        return f"{local_root.rstrip('/')}/{path_part}"

    def _parse_map_option(self, value: Optional[str]) -> Dict[str, str]:
        """解析 Map 类型的配置项。
        
        格式：key1:value1,key2:value2
        """
        if not value:
            return {}
        
        result = {}
        for item in value.split(','):
            item = item.strip()
            if ':' in item:
                key, val = item.split(':', 1)
                result[key.strip()] = val.strip()
        return result
```

---

## 【高】需求二：FUSE 安全校验机制

为防止本地路径被误配置或篡改，设计两层校验机制。

### 问题场景

以下场景可能导致 FUSE 本地路径配置错误，进而引发数据安全问题：

| 场景 | 问题描述 | 潜在风险 |
|------|---------|---------|
| **路径配置错误** | 用户将 `fuse.local-path.root` 配置为错误的挂载点，如将 `/mnt/fuse-a` 误配置为 `/mnt/fuse-b` | 读写到错误的数据目录，导致数据混乱或丢失 |
| **多租户环境混淆** | 同一机器上挂载了多个租户的 FUSE 路径，用户配置了错误的租户路径 | 跨租户数据泄露或越权访问 |
| **FUSE 挂载点漂移** | FUSE 进程重启后，挂载点路径发生变化但配置未更新 | 访问到其他表的数据，破坏数据一致性 |
| **恶意路径注入** | 攻击者通过篡改配置文件，将本地路径指向敏感数据目录 | 敏感数据泄露或被恶意修改 |
| **表路径重用** | 删除表后重建同名表，但 FUSE 挂载点仍指向旧数据 | 读写到过期数据，业务逻辑错误 |
| **并发挂载冲突** | 多个 FUSE 实例挂载到同一目录的不同状态 | 数据版本不一致，读写冲突 |

### 校验流程图

```
┌─────────────────────────────────────────────────────────────┐
│           FUSE 本地路径安全校验流程                          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────┐
        │ validation-mode == NONE ?             │
        └───────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                   Yes                 No
                    │                   │
                    ▼                   ▼
            ┌─────────────┐  ┌─────────────────────┐
            │ 跳过校验     │  │ 开始校验             │
            │ 直接使用     │  │                     │
            └─────────────┘  └─────────────────────┘
                                      │
                                      ▼
                    ┌─────────────────────────────────┐
                    │ 第一次校验：.identifier 文件    │
                    │ 比对远端和本地表标识 UUID       │
                    └─────────────────────────────────┘
                                      │
                          ┌───────────┴───────────┐
                         成功                   失败
                          │                       │
                          ▼                       ▼
            ┌─────────────────────┐  ┌───────────────────────┐
            │ 第二次校验：远端数据  │  │ 根据校验模式处理：      │
            │ 比对文件大小和哈希    │  │ - strict: 抛异常       │
            │                     │  │ - warn: 警告并回退     │
            └─────────────────────┘  └───────────────────────┘
                          │
                ┌─────────┴─────────┐
               成功                失败
                │                   │
                ▼                   ▼
        ┌─────────────┐  ┌───────────────────────┐
        │ 使用本地 IO  │  │ 根据校验模式处理        │
        └─────────────┘  └───────────────────────┘
```

### 配置参数

| 参数 | 类型 | 默认值 | 描述 |
|-----|------|--------|------|
| `fuse.local-path.validation-mode` | String | `strict` | 校验模式：`strict`、`warn` 或 `none` |

### 校验模式说明

| 模式 | 校验失败时行为 | 适用场景 |
|------|---------------|----------|
| `strict` | 抛出异常，阻止操作 | 生产环境，安全优先 |
| `warn` | 记录警告，回退到默认 FileIO | 测试环境，兼容性优先 |
| `none` | 不校验，直接使用 | 信任环境，性能优先 |

### FuseOptions 配置扩展

在 `pypaimon/common/options/config.py` 中添加：

```python
class FuseOptions:
    # ... 原有配置项 ...
    
    FUSE_LOCAL_PATH_VALIDATION_MODE = (
        ConfigOptions.key("fuse.local-path.validation-mode")
        .string_type()
        .default_value("strict")
        .with_description("校验模式：strict、warn 或 none")
    )
```

### Python 安全校验实现

在 `pypaimon/catalog/rest/rest_catalog.py` 中添加安全校验逻辑：

```python
import hashlib
import logging
from enum import Enum
from typing import Optional, Tuple
from pathlib import Path

class ValidationMode(Enum):
    """校验模式枚举。"""
    STRICT = "strict"  # 严格模式：校验失败抛异常
    WARN = "warn"      # 警告模式：校验失败只警告，回退到默认逻辑
    NONE = "none"      # 不校验


class ValidationResult:
    """校验结果类。"""
    
    def __init__(self, valid: bool, error_message: Optional[str] = None):
        self.valid = valid
        self.error_message = error_message

    @staticmethod
    def success() -> 'ValidationResult':
        return ValidationResult(True)

    @staticmethod
    def fail(error_message: str) -> 'ValidationResult':
        return ValidationResult(False, error_message)


class RESTCatalog(Catalog):
    # ... 原有代码 ...

    def _get_validation_mode(self) -> ValidationMode:
        """获取校验模式。"""
        mode_str = self.context.options.get(
            FuseOptions.FUSE_LOCAL_PATH_VALIDATION_MODE, "strict")
        return ValidationMode(mode_str.lower())

    def _validate_fuse_path(self, local_path: str, remote_path: str, 
                            identifier: Identifier) -> ValidationResult:
        """校验 FUSE 本地路径。"""
        local_file_io = LocalFileIO(local_path, self.context.options)
        logger = logging.getLogger(__name__)

        # 1. 检查本地路径是否存在
        if not local_file_io.exists(local_path):
            return ValidationResult.fail(f"本地路径不存在: {local_path}")

        # 2. 第一次校验：表标识文件
        identifier_result = self._validate_by_identifier_file(
            local_file_io, local_path, remote_path, identifier)
        if not identifier_result.valid:
            return identifier_result

        # 3. 第二次校验：远端数据校验
        return self._validate_by_remote_data(
            local_file_io, local_path, remote_path, identifier)

    def _validate_by_identifier_file(self, local_file_io: LocalFileIO,
                                     local_path: str, remote_path: str,
                                     identifier: Identifier) -> ValidationResult:
        """第一次校验：检查 .identifier 文件。"""
        logger = logging.getLogger(__name__)
        
        try:
            # 获取远端存储 FileIO
            remote_file_io = self._create_default_file_io(remote_path, identifier)

            # 读取远端标识文件
            remote_identifier_file = f"{remote_path.rstrip('/')}/.identifier"
            if not remote_file_io.exists(remote_identifier_file):
                logger.debug(f"未找到表 {identifier} 的 .identifier 文件，跳过标识校验")
                return ValidationResult.success()

            remote_identifier = self._read_identifier_file(remote_file_io, remote_identifier_file)

            # 读取本地标识文件
            local_identifier_file = f"{local_path.rstrip('/')}/.identifier"
            if not local_file_io.exists(local_identifier_file):
                return ValidationResult.fail(
                    f"本地 .identifier 文件未找到: {local_identifier_file}。"
                    "FUSE 路径可能未正确挂载。")

            local_identifier = self._read_identifier_file(local_file_io, local_identifier_file)

            # 比对标识符
            if remote_identifier != local_identifier:
                return ValidationResult.fail(
                    f"表标识不匹配！本地: {local_identifier}，远端: {remote_identifier}。"
                    "本地路径可能指向了其他表。")

            return ValidationResult.success()

        except Exception as e:
            logger.warning(f"标识文件校验失败: {identifier}", exc_info=True)
            return ValidationResult.fail(f"标识文件校验失败: {str(e)}")

    def _read_identifier_file(self, file_io: FileIO, identifier_file: str) -> str:
        """读取 .identifier 文件内容。"""
        import json
        content = file_io.read_file_utf8(identifier_file)
        data = json.loads(content)
        return data.get("uuid", "")

    def _validate_by_remote_data(self, local_file_io: LocalFileIO,
                                 local_path: str, remote_path: str,
                                 identifier: Identifier) -> ValidationResult:
        """第二次校验：通过比对远端存储和本地文件验证 FUSE 路径正确性。"""
        logger = logging.getLogger(__name__)
        
        try:
            # 获取远端存储 FileIO
            remote_file_io = self._create_default_file_io(remote_path, identifier)

            # 使用 SnapshotManager 获取最新 snapshot
            from pypaimon.snapshot.snapshot_manager import SnapshotManager
            snapshot_manager = SnapshotManager(remote_file_io, remote_path)
            latest_snapshot = snapshot_manager.latest_snapshot()

            if latest_snapshot is not None:
                checksum_file = snapshot_manager.snapshot_path(latest_snapshot.id())
            else:
                # 无 snapshot（新表），使用 schema 文件校验
                from pypaimon.schema.schema_manager import SchemaManager
                schema_manager = SchemaManager(remote_file_io, remote_path)
                latest_schema = schema_manager.latest()
                
                if latest_schema is None:
                    logger.info(f"未找到表 {identifier} 的 snapshot 或 schema，跳过验证")
                    return ValidationResult.success()
                    
                checksum_file = schema_manager.to_schema_path(latest_schema.id())

            # 读取远端文件信息
            remote_status = remote_file_io.get_file_status(checksum_file)
            remote_hash = self._compute_file_hash(remote_file_io, checksum_file)

            # 构建本地文件路径
            from urllib.parse import urlparse
            uri = urlparse(checksum_file)
            path_part = uri.path.lstrip('/')
            local_checksum_file = f"{local_path.rstrip('/')}/{path_part}"

            if not local_file_io.exists(local_checksum_file):
                return ValidationResult.fail(
                    f"本地文件未找到: {local_checksum_file}。"
                    "FUSE 路径可能未正确挂载。")

            local_size = local_file_io.get_file_size(local_checksum_file)
            local_hash = self._compute_file_hash(local_file_io, local_checksum_file)

            # 比对文件特征
            if local_size != remote_status.size:
                return ValidationResult.fail(
                    f"文件大小不匹配！本地: {local_size} 字节, 远端: {remote_status.size} 字节。")

            if local_hash.lower() != remote_hash.lower():
                return ValidationResult.fail(
                    f"文件内容哈希不匹配！本地: {local_hash}, 远端: {remote_hash}。")

            return ValidationResult.success()

        except Exception as e:
            logger.warning(f"通过远端数据验证 FUSE 路径失败: {identifier}", exc_info=True)
            return ValidationResult.fail(f"远端数据验证失败: {str(e)}")

    def _compute_file_hash(self, file_io: FileIO, file_path: str) -> str:
        """计算文件内容的 MD5 哈希。"""
        md5 = hashlib.md5()
        with file_io.new_input_stream(file_path) as input_stream:
            while True:
                data = input_stream.read(4096)
                if not data:
                    break
                md5.update(data)
        return md5.hexdigest()

    def _handle_validation_error(self, result: ValidationResult, mode: ValidationMode):
        """处理校验错误。"""
        error_msg = f"FUSE local path validation failed: {result.error_message}"
        logger = logging.getLogger(__name__)

        if mode == ValidationMode.STRICT:
            raise ValueError(error_msg)
        elif mode == ValidationMode.WARN:
            logger.warning(f"{error_msg}. Falling back to default FileIO.")

    def _create_default_file_io(self, path: str, identifier: Identifier) -> FileIO:
        """创建默认 FileIO（原有逻辑）。"""
        return RESTTokenFileIO(identifier, path, self.context.options) \
            if self.data_token_enabled else self.file_io_from_options(path)
```

### RESTCatalog 集成校验

在 `file_io_for_data` 方法中集成校验逻辑：

```python
class RESTCatalog(Catalog):
    def __init__(self, context: CatalogContext, config_required: Optional[bool] = True):
        # ... 原有初始化代码 ...
        self.fuse_local_path_enabled = self.context.options.get(
            FuseOptions.FUSE_LOCAL_PATH_ENABLED, False)
        self._validation_mode_cache: Optional[ValidationMode] = None

    def file_io_for_data(self, table_path: str, identifier: Identifier) -> FileIO:
        """获取用于数据访问的 FileIO，支持 FUSE 本地路径映射。"""
        # 1. 尝试解析 FUSE 本地路径
        if self.fuse_local_path_enabled:
            local_path = self._resolve_fuse_local_path(table_path, identifier)
            if local_path is not None:
                # 2. 如果需要，执行校验
                validation_mode = self._get_validation_mode()
                if validation_mode != ValidationMode.NONE:
                    result = self._validate_fuse_path(local_path, table_path, identifier)
                    if not result.valid:
                        self._handle_validation_error(result, validation_mode)
                        return self._create_default_file_io(table_path, identifier)

                # 3. 返回本地 FileIO
                return LocalFileIO(local_path, self.context.options)

        # 4. 回退到原有逻辑
        return RESTTokenFileIO(identifier, table_path, self.context.options) \
            if self.data_token_enabled else self.file_io_from_options(table_path)
```

### 校验优势

| 优势 | 说明 |
|------|------|
| 双重校验 | 先校验表标识，再校验数据一致性 |
| 防止路径混淆 | 通过 UUID 确保本地路径指向正确的表 |
| 灵活模式 | strict/warn/none 三种模式适应不同场景 |
| 性能优化 | 仅在启用时执行校验，不影响正常路径 |

---

## 【低】需求三：FUSE 错误处理

FUSE 挂载可能出现特有错误，需要特殊处理以保证系统稳定性。

### 常见 FUSE 错误

| 错误类型 | 错误信息 | 原因 | 处理策略 |
|---------|---------|------|---------|
| 挂载断开 | `Transport endpoint is not connected` | FUSE 进程崩溃或挂载失效 | 立即失败，不重试 |
| 过期文件句柄 | `Stale file handle` | 文件被其他进程修改或删除 | 指数退避重试 |
| 设备忙 | `Device or resource busy` | 资源竞争 | 指数退避重试 |

### 错误处理流程图

```
┌─────────────────────────────────────────────────────────────┐
│                    FUSE 错误处理流程                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────┐
        │ 执行文件操作                          │
        └───────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                   成功                失败
                    │                   │
                    ▼                   ▼
            ┌─────────────┐  ┌─────────────────────┐
            │ 返回结果     │  │ 检查错误类型         │
            └─────────────┘  └─────────────────────┘
                                      │
                              ┌───────┴───────┐
                              │               │
                              ▼               ▼
        ┌─────────────────────────────┐  ┌─────────────────────────────┐
        │ FUSE 挂载断开？              │  │ 其他错误                    │
        │ "Transport endpoint is not   │  │                             │
        │  connected"                  │  │                             │
        └─────────────────────────────┘  └─────────────────────────────┘
                    │                               │
                   Yes                              │
                    │                               │
                    ▼                               ▼
        ┌─────────────────────────────┐  ┌─────────────────────────────┐
        │ LOG.error("FUSE 挂载断开")   │  │ 直接抛出异常                │
        │ 抛出异常，不重试             │  └─────────────────────────────┘
        └─────────────────────────────┘
                    
        ┌───────────────────────────────────────┐
        │ Stale file handle 或 Device busy ?    │
        └───────────────────────────────────────┘
                              │
                             Yes
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ LOG.warn("Stale file handle: {}, 重试中...", path)         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────┐
        │ 重试一次: 重新打开文件                 │
        └───────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                   成功                失败
                    │                   │
                    ▼                   ▼
            ┌─────────────┐  ┌─────────────────────┐
            │ 继续操作     │  │ 抛出 OSError         │
            │             │  │ 并附带详细信息        │
            └─────────────┘  └─────────────────────┘
```

### 配置参数

| 参数 | 类型 | 默认值 | 描述 |
|-----|------|--------|------|
| `fuse.local-path.retry.max-attempts` | Integer | `3` | FUSE 特有错误的最大重试次数（如 stale file handle） |
| `fuse.local-path.retry.initial-delay-ms` | Integer | `100` | 初始重试延迟（毫秒） |
| `fuse.local-path.retry.max-delay-ms` | Integer | `5000` | 最大重试延迟（毫秒） |

### FuseOptions 配置扩展

在 `pypaimon/common/options/config.py` 中添加：

```python
class FuseOptions:
    # ... 原有配置项 ...
    
    FUSE_LOCAL_PATH_RETRY_MAX_ATTEMPTS = (
        ConfigOptions.key("fuse.local-path.retry.max-attempts")
        .int_type()
        .default_value(3)
        .with_description("FUSE 特有错误的最大重试次数（如 stale file handle）")
    )
    
    FUSE_LOCAL_PATH_RETRY_INITIAL_DELAY_MS = (
        ConfigOptions.key("fuse.local-path.retry.initial-delay-ms")
        .int_type()
        .default_value(100)
        .with_description("初始重试延迟（毫秒）")
    )
    
    FUSE_LOCAL_PATH_RETRY_MAX_DELAY_MS = (
        ConfigOptions.key("fuse.local-path.retry.max-delay-ms")
        .int_type()
        .default_value(5000)
        .with_description("最大重试延迟（毫秒）")
    )
```

### Python FUSE 错误处理实现

在 `pypaimon/filesystem/fuse_aware_file_io.py` 中创建 FUSE 错误处理器：

```python
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
import time
from typing import Callable, TypeVar, Optional

from pypaimon.common.file_io import FileIO
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.common.options import Options
from pypaimon.common.options.config import FuseOptions


T = TypeVar('T')
logger = logging.getLogger(__name__)


class FuseErrorHandler:
    """FUSE 错误处理器，支持可配置的重试和指数退避。"""

    def __init__(self, max_attempts: int = 3, initial_delay_ms: int = 100, 
                 max_delay_ms: int = 5000):
        self.max_attempts = max_attempts
        self.initial_delay_ms = initial_delay_ms
        self.max_delay_ms = max_delay_ms

    def is_fuse_mount_disconnected(self, error: OSError) -> bool:
        """检查是否为 FUSE 挂载断开错误。"""
        msg = str(error)
        return "Transport endpoint is not connected" in msg

    def is_stale_file_handle(self, error: OSError) -> bool:
        """检查是否为 Stale file handle 错误。"""
        msg = str(error)
        return "Stale file handle" in msg

    def is_device_busy(self, error: OSError) -> bool:
        """检查是否为 Device or resource busy 错误。"""
        msg = str(error)
        return "Device or resource busy" in msg

    def calculate_delay(self, attempt: int) -> float:
        """计算指数退避延迟（秒）。"""
        delay = self.initial_delay_ms * (2 ** attempt)
        return min(delay, self.max_delay_ms) / 1000.0  # 转换为秒

    def execute_with_fuse_error_handling(
        self, 
        operation: Callable[[], T], 
        path: str, 
        operation_name: str
    ) -> T:
        """执行文件操作，处理 FUSE 特有错误。"""
        last_exception: Optional[Exception] = None

        for attempt in range(self.max_attempts):
            try:
                return operation()
            except OSError as e:
                last_exception = e

                # FUSE 挂载断开 - 立即失败，不重试
                if self.is_fuse_mount_disconnected(e):
                    logger.error(
                        f"FUSE 挂载断开，路径: {path}。请检查: "
                        "1) FUSE 进程是否运行, 2) 挂载点是否存在, 3) 如需重新挂载"
                    )
                    raise OSError(f"FUSE 挂载断开: {path}") from e

                # 可重试错误: stale file handle, device busy
                if self.is_stale_file_handle(e) or self.is_device_busy(e):
                    if attempt < self.max_attempts - 1:
                        delay = self.calculate_delay(attempt)
                        logger.warning(
                            f"FUSE 错误 ({e}) 路径: {path}, "
                            f"{delay * 1000:.0f}ms 后重试 (第 {attempt + 1}/{self.max_attempts} 次)"
                        )
                        time.sleep(delay)
                        continue

                # 不可重试错误或达到最大重试次数
                raise

        # 不应到达这里，但以防万一
        raise OSError(
            f"FUSE 操作失败，已重试 {self.max_attempts} 次: {path}"
        ) from last_exception


class FuseAwareFileIO(FileIO):
    """FileIO 包装器，处理 FUSE 特有错误，支持可配置重试。
    
    委托给 LocalFileIO 执行实际文件操作。
    """

    def __init__(self, fuse_path: str, catalog_options: Optional[Options] = None):
        self.delegate = LocalFileIO(fuse_path, catalog_options)
        
        options = catalog_options or Options({})
        self.error_handler = FuseErrorHandler(
            max_attempts=options.get(FuseOptions.FUSE_LOCAL_PATH_RETRY_MAX_ATTEMPTS, 3),
            initial_delay_ms=options.get(FuseOptions.FUSE_LOCAL_PATH_RETRY_INITIAL_DELAY_MS, 100),
            max_delay_ms=options.get(FuseOptions.FUSE_LOCAL_PATH_RETRY_MAX_DELAY_MS, 5000)
        )

    def new_input_stream(self, path: str):
        return self.error_handler.execute_with_fuse_error_handling(
            lambda: self.delegate.new_input_stream(path), path, "new_input_stream"
        )

    def new_output_stream(self, path: str):
        return self.error_handler.execute_with_fuse_error_handling(
            lambda: self.delegate.new_output_stream(path), path, "new_output_stream"
        )

    def get_file_status(self, path: str):
        return self.error_handler.execute_with_fuse_error_handling(
            lambda: self.delegate.get_file_status(path), path, "get_file_status"
        )

    def list_status(self, path: str):
        return self.error_handler.execute_with_fuse_error_handling(
            lambda: self.delegate.list_status(path), path, "list_status"
        )

    def exists(self, path: str) -> bool:
        return self.error_handler.execute_with_fuse_error_handling(
            lambda: self.delegate.exists(path), path, "exists"
        )

    def delete(self, path: str, recursive: bool = False) -> bool:
        return self.error_handler.execute_with_fuse_error_handling(
            lambda: self.delegate.delete(path, recursive), path, "delete"
        )

    def mkdirs(self, path: str) -> bool:
        return self.error_handler.execute_with_fuse_error_handling(
            lambda: self.delegate.mkdirs(path), path, "mkdirs"
        )

    def rename(self, src: str, dst: str) -> bool:
        return self.error_handler.execute_with_fuse_error_handling(
            lambda: self.delegate.rename(src, dst), src, "rename"
        )

    def to_filesystem_path(self, path: str) -> str:
        return self.delegate.to_filesystem_path(path)

    def write_parquet(self, path: str, data, compression: str = 'zstd',
                      zstd_level: int = 1, **kwargs):
        return self.error_handler.execute_with_fuse_error_handling(
            lambda: self.delegate.write_parquet(path, data, compression, zstd_level, **kwargs),
            path, "write_parquet"
        )

    def write_orc(self, path: str, data, compression: str = 'zstd',
                  zstd_level: int = 1, **kwargs):
        return self.error_handler.execute_with_fuse_error_handling(
            lambda: self.delegate.write_orc(path, data, compression, zstd_level, **kwargs),
            path, "write_orc"
        )

    def write_avro(self, path: str, data, avro_schema=None,
                   compression: str = 'zstd', zstd_level: int = 1, **kwargs):
        return self.error_handler.execute_with_fuse_error_handling(
            lambda: self.delegate.write_avro(path, data, avro_schema, compression, zstd_level, **kwargs),
            path, "write_avro"
        )

    @property
    def filesystem(self):
        return self.delegate.filesystem

    @property
    def uri_reader_factory(self):
        return self.delegate.uri_reader_factory

    def close(self):
        self.delegate.close()
```

### RESTCatalog 集成 FuseAwareFileIO

在 `pypaimon/catalog/rest/rest_catalog.py` 中集成：

```python
from pypaimon.filesystem.fuse_aware_file_io import FuseAwareFileIO

class RESTCatalog(Catalog):
    def file_io_for_data(self, table_path: str, identifier: Identifier) -> FileIO:
        """获取用于数据访问的 FileIO，支持 FUSE 本地路径映射。"""
        # 1. 尝试解析 FUSE 本地路径
        if self.fuse_local_path_enabled:
            local_path = self._resolve_fuse_local_path(table_path, identifier)
            if local_path is not None:
                # 2. 如果需要，执行校验
                validation_mode = self._get_validation_mode()
                if validation_mode != ValidationMode.NONE:
                    result = self._validate_fuse_path(local_path, table_path, identifier)
                    if not result.valid:
                        self._handle_validation_error(result, validation_mode)
                        return self._create_default_file_io(table_path, identifier)

                # 3. 返回带错误处理的 FuseAwareFileIO
                return FuseAwareFileIO(local_path, self.context.options)

        # 4. 回退到原有逻辑
        return RESTTokenFileIO(identifier, table_path, self.context.options) \
            if self.data_token_enabled else self.file_io_from_options(table_path)
```

### 重试行为示例

| 错误类型 | 是否重试 | 延迟模式（默认设置） |
|----------|----------|---------------------|
| `Transport endpoint is not connected` | ❌ 否 | 立即失败 |
| `Stale file handle` | ✅ 是 | 100ms → 200ms → 400ms → 失败 |
| `Device or resource busy` | ✅ 是 | 100ms → 200ms → 400ms → 失败 |
| 其他 OSError | ❌ 否 | 直接抛出 |

### 日志规范

| 日志级别 | 场景 | 示例 |
|----------|------|------|
| ERROR | FUSE 挂载断开 | `FUSE 挂载断开，路径: /mnt/fuse/db/table/snapshot-1` |
| WARN | Stale file handle | `Stale file handle: /mnt/fuse/..., 重试一次...` |
| INFO | 正常 FUSE 操作 | （可选，用于调试） |

### FUSE 用户最佳实践

1. **监控 FUSE 进程**：使用 `ps aux | grep fusermount` 或 FUSE 工具的监控功能
2. **健康检查**：定期使用 `ls` 或 `stat` 检查挂载点
3. **自动重启**：考虑使用 systemd 或 supervisor 在崩溃时自动重启 FUSE
4. **日志分析**：查看 `dmesg` 或 FUSE 日志进行根因分析

---

## 文件结构

新增/修改的文件：

```
paimon-python/
├── pypaimon/
│   ├── catalog/
│   │   └── rest/
│   │       └── rest_catalog.py      # 修改：添加 FUSE 支持
│   ├── common/
│   │   └── options/
│   │       └── config.py            # 修改：添加 FuseOptions
│   └── filesystem/
│       └── fuse_aware_file_io.py    # 新增：FUSE 错误处理 FileIO
```

## 总结

本设计为 PyPaimon 的 RESTCatalog 提供 FUSE 本地路径支持，主要特性：

1. **分层路径映射**：支持 Catalog、Database、Table 三级路径配置
2. **安全校验**：双重校验机制（表标识 + 数据一致性）防止路径配置错误
3. **错误处理**：针对 FUSE 特有错误（stale file handle 等）的重试机制
4. **向后兼容**：默认禁用，不影响现有功能
5. **灵活配置**：三种校验模式（strict/warn/none）适应不同场景
