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

# 简化版 FUSE 本地路径映射设计

## 背景

在使用 Paimon RESTCatalog 访问远端对象存储（如 OSS、S3、HDFS）时，数据访问通常通过远端存储 SDK 进行。然而，在远端存储路径通过 FUSE（用户空间文件系统）挂载到本地的场景下，用户可以直接通过本地文件系统路径访问数据，获得更好的性能。

本文档描述一个**简化版本**的实现，仅支持 Catalog 级别挂载，通过检查 `default` 数据库实现校验。

## 目标

1. 仅支持 **Catalog 级别**挂载（单一 `fuse.local-path.root` 配置）
2. 校验通过检查 `default` 数据库的 `location` 与本地路径是否匹配
3. 保持与现有 RESTCatalog 行为的向后兼容性

---

## 配置参数

所有参数定义在 `pypaimon/common/options/config.py` 中的 `FuseOptions` 类：

| 参数 | 类型 | 默认值 | 描述 |
|-----|------|--------|------|
| `fuse.local-path.enabled` | Boolean | `false` | 是否启用 FUSE 本地路径映射 |
| `fuse.local-path.root` | String | 无 | FUSE 挂载的本地根路径，如 `/mnt/fuse/warehouse` |
| `fuse.local-path.validation-mode` | String | `strict` | 校验模式：`strict`、`warn` 或 `none` |

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
    'fuse.local-path.validation-mode': 'strict'
})
```

---

## 校验模式说明

**校验模式仅用于校验 FUSE 挂载是否正确**（检查本地路径是否存在），不处理配置错误。

| 模式 | 校验失败时行为 | 适用场景 |
|------|---------------|----------|
| `strict` | 抛出异常，阻止操作 | 生产环境，安全优先 |
| `warn` | 记录警告，回退到默认 FileIO | 测试环境，兼容性优先 |
| `none` | 不校验，直接使用 | 信任环境，性能优先 |

**配置错误**（如 `fuse.local-path.enabled=true` 但 `fuse.local-path.root` 未配置）直接抛异常，不走校验模式。

---

## 校验流程图

```
┌─────────────────────────────────────────────────────────────┐
│              FUSE 本地路径校验流程                           │
│              （首次访问数据时触发）                           │
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
            ┌─────────────┐  ┌─────────────────────────────┐
            │ 跳过校验     │  │ 调用 get_database("default") │
            │ state=True  │  └─────────────────────────────┘
            └─────────────┘                │
                                           ▼
                          ┌─────────────────────────────────┐
                          │ 获取 default 数据库的 location   │
                          │ 转换为本地 FUSE 路径             │
                          │ 检查本地路径是否存在             │
                          └─────────────────────────────────┘
                                           │
                               ┌───────────┴───────────┐
                            存在                  不存在
                              │                       │
                              ▼                       ▼
                    ┌─────────────────┐  ┌─────────────────────────┐
                    │ 校验通过         │  │ 根据 validation-mode:   │
                    │ state=True      │  │ - strict: 抛出异常      │
                    └─────────────────┘  │ - warn: state=False     │
                                         └─────────────────────────┘
```

---

## 路径转换逻辑

将远程存储路径转换为本地 FUSE 路径，需要跳过 catalog 层级：

```python
def _resolve_fuse_local_path(self, original_path: str) -> str:
    """
    将远程存储路径转换为本地 FUSE 路径。

    示例:
    - 输入: oss://catalog/db1/table1
    - fuse_root: /mnt/fuse/warehouse
    - 输出: /mnt/fuse/warehouse/db1/table1

    说明: FUSE 挂载点已映射到 catalog 层级，因此需要跳过路径中的 catalog 名称。

    Args:
        original_path: 原始远程存储路径

    Returns:
        本地 FUSE 路径

    Raises:
        ValueError: 如果 fuse.local-path.root 未配置
    """
    if not self.fuse_local_path_root:
        raise ValueError(
            "FUSE local path is enabled but fuse.local-path.root is not configured"
        )

    uri = urlparse(original_path)

    # 提取路径部分
    # 有 scheme 时（如 oss://catalog/db/table）：
    #   - netloc 是 bucket 名（对应 catalog 名）
    #   - path 是剩余部分（如 /db/table）
    #   - 跳过 netloc，只保留 path
    # 无 scheme 时（如 catalog/db/table）：
    #   - 跳过第一段（catalog 名）
    if uri.scheme:
        # 跳过 netloc（bucket/catalog），只保留 path 部分
        path_part = uri.path.lstrip('/')
    else:
        # 无 scheme：路径格式为 "catalog/db/table"，跳过第一段
        path_part = original_path.lstrip('/')
        segments = path_part.split('/')
        if len(segments) > 1:
            path_part = '/'.join(segments[1:])

    return f"{self.fuse_local_path_root.rstrip('/')}/{path_part}"
```

---

## 行为矩阵

| 配置 | 校验结果 | 行为 |
|-----|---------|------|
| `enabled=true, mode=strict` | 通过 | 使用 LocalFileIO |
| `enabled=true, mode=strict` | 失败 | 抛出 ValueError |
| `enabled=true, mode=warn` | 通过 | 使用 LocalFileIO |
| `enabled=true, mode=warn` | 失败 | 警告，回退到默认 FileIO |
| `enabled=true, mode=none` | - | 直接使用 LocalFileIO |
| `enabled=false` | - | 使用原有逻辑（data token 或 FileIO.get） |

---

## 代码改动清单

### 1. 新增配置类 FuseOptions

**文件**: `pypaimon/common/options/config.py`

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

    FUSE_LOCAL_PATH_VALIDATION_MODE = (
        ConfigOptions.key("fuse.local-path.validation-mode")
        .string_type()
        .default_value("strict")
        .with_description("校验模式：strict、warn 或 none")
    )
```

### 2. 修改 RESTCatalog

**文件**: `pypaimon/catalog/rest/rest_catalog.py`

#### 2.1 新增导入

```python
import logging
from urllib.parse import urlparse
from pypaimon.common.options.config import FuseOptions
from pypaimon.filesystem.local_file_io import LocalFileIO

logger = logging.getLogger(__name__)
```

#### 2.2 修改 `__init__` 方法

```python
def __init__(self, context: CatalogContext, config_required: Optional[bool] = True):
    # ... 原有初始化代码 ...
    self.data_token_enabled = self.rest_api.options.get(CatalogOptions.DATA_TOKEN_ENABLED)

    # FUSE 本地路径配置
    self.fuse_local_path_enabled = self.context.options.get(
        FuseOptions.FUSE_LOCAL_PATH_ENABLED, False)
    self.fuse_local_path_root = self.context.options.get(
        FuseOptions.FUSE_LOCAL_PATH_ROOT)
    self.fuse_validation_mode = self.context.options.get(
        FuseOptions.FUSE_LOCAL_PATH_VALIDATION_MODE, "strict")
    self._fuse_validation_state = None  # None=未校验, True=通过, False=失败
```

#### 2.3 修改 `file_io_for_data` 方法

```python
def file_io_for_data(self, table_path: str, identifier: Identifier) -> FileIO:
    """
    获取用于数据访问的 FileIO，支持 FUSE 本地路径映射。
    """
    # 尝试使用 FUSE 本地路径
    if self.fuse_local_path_enabled:
        # 配置错误直接抛异常
        local_path = self._resolve_fuse_local_path(table_path)

        # 执行校验（仅首次）
        if self._fuse_validation_state is None:
            self._validate_fuse_path()

        # 校验通过，返回本地 FileIO
        if self._fuse_validation_state:
            return LocalFileIO(local_path, self.context.options)

        # warn 模式校验失败后回退
        return RESTTokenFileIO(identifier, table_path, self.context.options) \
            if self.data_token_enabled else self.file_io_from_options(table_path)

    # 回退到原有逻辑
    return RESTTokenFileIO(identifier, table_path, self.context.options) \
        if self.data_token_enabled else self.file_io_from_options(table_path)
```

#### 2.4 新增 `_resolve_fuse_local_path` 方法

```python
def _resolve_fuse_local_path(self, original_path: str) -> str:
    """
    解析 FUSE 本地路径。

    FUSE 挂载点已映射到 catalog 层级，需要跳过路径中的 catalog 名称。

    Returns:
        本地路径

    Raises:
        ValueError: 如果 fuse.local-path.root 未配置
    """
    if not self.fuse_local_path_root:
        raise ValueError(
            "FUSE local path is enabled but fuse.local-path.root is not configured"
        )

    uri = urlparse(original_path)

    # 提取路径部分
    # 有 scheme 时（如 oss://catalog/db/table）：
    #   - netloc 是 bucket 名（对应 catalog 名）
    #   - path 是剩余部分（如 /db/table）
    #   - 跳过 netloc，只保留 path
    # 无 scheme 时（如 catalog/db/table）：
    #   - 跳过第一段（catalog 名）
    if uri.scheme:
        # 跳过 netloc（bucket/catalog），只保留 path 部分
        path_part = uri.path.lstrip('/')
    else:
        # 无 scheme：路径格式为 "catalog/db/table"，跳过第一段
        path_part = original_path.lstrip('/')
        segments = path_part.split('/')
        if len(segments) > 1:
            path_part = '/'.join(segments[1:])

    return f"{self.fuse_local_path_root.rstrip('/')}/{path_part}"
```

#### 2.5 新增 `_validate_fuse_path` 方法

```python
def _validate_fuse_path(self) -> None:
    """
    校验 FUSE 本地路径是否正确挂载。

    获取 default 数据库的 location，转换为本地路径后检查是否存在。
    """
    if self.fuse_validation_mode == "none":
        self._fuse_validation_state = True
        return

    # 获取 default 数据库详情，API 调用失败直接抛异常
    db = self.rest_api.get_database("default")
    remote_location = db.location

    if not remote_location:
        logger.info("Default database has no location, skipping FUSE validation")
        self._fuse_validation_state = True
        return

    expected_local = self._resolve_fuse_local_path(remote_location)
    local_file_io = LocalFileIO(expected_local, self.context.options)

    # 只校验本地路径是否存在，根据 validation mode 处理
    if not local_file_io.exists(expected_local):
        error_msg = (
            f"FUSE local path validation failed: "
            f"local path '{expected_local}' does not exist "
            f"for default database location '{remote_location}'"
        )
        self._handle_validation_error(error_msg)
    else:
        self._fuse_validation_state = True
        logger.info("FUSE local path validation passed")
```

**说明**：
- 只负责校验 FUSE 是否正确挂载，不处理配置错误
- API 调用失败等系统异常直接抛出，不走校验模式
- 直接调用 `get_database("default")` 获取 default 数据库
- 将远端 location 转换为本地 FUSE 路径后检查是否存在
- 简单高效，只需一次 API 调用

#### 2.6 新增 `_handle_validation_error` 方法

```python
def _handle_validation_error(self, error_msg: str) -> None:
    """根据校验模式处理错误。"""
    if self.fuse_validation_mode == "strict":
        raise ValueError(error_msg)
    elif self.fuse_validation_mode == "warn":
        logger.warning(f"{error_msg}. Falling back to default FileIO.")
        self._fuse_validation_state = False  # 标记校验失败，回退到默认 FileIO
```

---

## 文件结构

```
paimon-python/
├── pypaimon/
│   ├── catalog/
│   │   └── rest/
│   │       └── rest_catalog.py      # 修改：添加 FUSE 支持
│   └── common/
│       └── options/
│           └── config.py            # 修改：添加 FuseOptions
```

---

## 与完整版设计的区别

| 特性 | 简化版 | 完整版 |
|------|--------|--------|
| 挂载级别 | 仅 Catalog 级别 | Catalog / Database / Table 三级 |
| 配置项 | 3 个 | 7+ 个 |
| 校验方式 | 检查 default 数据库路径存在性 | 双重校验（标识文件 + 数据哈希） |
| 错误处理 | 基本模式 | FUSE 特有错误重试机制 |
| 复杂度 | 低 | 高 |

---

## 测试用例

### 1. 基本功能测试

```python
def test_fuse_local_path_basic():
    """测试基本 FUSE 本地路径功能"""
    options = {
        'metastore': 'rest',
        'uri': 'http://localhost:8080',
        'token': 'xxx',
        'fuse.local-path.enabled': 'true',
        'fuse.local-path.root': '/mnt/fuse/warehouse',
    }
    catalog = Catalog.create(options)
    # 验证 FUSE 配置已加载
    assert catalog.fuse_local_path_enabled == True
```

### 2. 校验模式测试

```python
def test_validation_mode_strict():
    """测试 strict 模式校验失败抛出异常"""
    # 配置不存在的 FUSE 路径
    # 预期抛出 ValueError

def test_validation_mode_warn():
    """测试 warn 模式校验失败回退"""
    # 配置不存在的 FUSE 路径
    # 预期使用默认 FileIO

def test_validation_mode_none():
    """测试 none 模式跳过校验"""
    # 配置不存在的 FUSE 路径
    # 预期直接使用 LocalFileIO
```

### 3. 边界条件测试

```python
def test_default_db_no_location():
    """测试 default 数据库没有 location 的情况"""

def test_default_db_not_exist():
    """测试 default 数据库不存在的情况"""

def test_resolve_fuse_path_missing_root():
    """测试启用 FUSE 但未配置 root 时报错"""

def test_disabled_fuse():
    """测试 FUSE 未启用时使用默认逻辑"""
```

### 4. `_resolve_fuse_local_path` 路径转换测试

```python
def test_resolve_fuse_local_path_basic():
    """测试基本路径转换"""
    # 输入: oss://catalog/db1/table1
    # fuse_root: /mnt/fuse/warehouse
    # 预期输出: /mnt/fuse/warehouse/db1/table1

def test_resolve_fuse_local_path_with_trailing_slash():
    """测试 fuse_root 带尾部斜杠"""
    # 输入: oss://catalog/db1/table1
    # fuse_root: /mnt/fuse/warehouse/
    # 预期输出: /mnt/fuse/warehouse/db1/table1

def test_resolve_fuse_local_path_deep_path():
    """测试深层路径"""
    # 输入: oss://catalog/db1/table1/partition1/file.parquet
    # fuse_root: /mnt/fuse/warehouse
    # 预期输出: /mnt/fuse/warehouse/db1/table1/partition1/file.parquet

def test_resolve_fuse_local_path_without_scheme():
    """测试路径没有 scheme"""
    # 输入: catalog/db1/table1
    # fuse_root: /mnt/fuse/warehouse
    # 预期输出: /mnt/fuse/warehouse/db1/table1
```
