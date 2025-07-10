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

from abc import ABC
from typing import Dict, Optional, Any
from dataclasses import dataclass, field
import json
from datetime import datetime


class RESTResponse(ABC):
    """REST 响应基类"""
    pass


@dataclass
class AuditRESTResponse(RESTResponse):
    """审计 REST 响应基类"""

    # 字段常量
    FIELD_OWNER = "owner"
    FIELD_CREATED_AT = "createdAt"
    FIELD_CREATED_BY = "createdBy"
    FIELD_UPDATED_AT = "updatedAt"
    FIELD_UPDATED_BY = "updatedBy"

    owner: Optional[str] = None
    created_at: Optional[int] = None
    created_by: Optional[str] = None
    updated_at: Optional[int] = None
    updated_by: Optional[str] = None

    def get_owner(self) -> Optional[str]:
        """获取所有者"""
        return self.owner

    def get_created_at(self) -> Optional[int]:
        """获取创建时间"""
        return self.created_at

    def get_created_by(self) -> Optional[str]:
        """获取创建者"""
        return self.created_by

    def get_updated_at(self) -> Optional[int]:
        """获取更新时间"""
        return self.updated_at

    def get_updated_by(self) -> Optional[str]:
        """获取更新者"""
        return self.updated_by

    def get_created_datetime(self) -> Optional[datetime]:
        """获取创建时间的 datetime 对象"""
        if self.created_at:
            return datetime.fromtimestamp(self.created_at / 1000)
        return None

    def get_updated_datetime(self) -> Optional[datetime]:
        """获取更新时间的 datetime 对象"""
        if self.updated_at:
            return datetime.fromtimestamp(self.updated_at / 1000)
        return None


@dataclass
class GetDatabaseResponse(AuditRESTResponse):
    """获取数据库响应"""

    # 字段常量
    FIELD_ID = "id"
    FIELD_NAME = "name"
    FIELD_LOCATION = "location"
    FIELD_OPTIONS = "options"

    id: Optional[str] = None
    name: Optional[str] = None
    location: Optional[str] = None
    options: Optional[Dict[str, str]] = field(default_factory=dict)

    def __init__(self,
                 id: Optional[str] = None,
                 name: Optional[str] = None,
                 location: Optional[str] = None,
                 options: Optional[Dict[str, str]] = None,
                 owner: Optional[str] = None,
                 created_at: Optional[int] = None,
                 created_by: Optional[str] = None,
                 updated_at: Optional[int] = None,
                 updated_by: Optional[str] = None):
        """
        初始化获取数据库响应

        Args:
            id: 数据库 ID
            name: 数据库名称
            location: 数据库位置
            options: 数据库选项
            owner: 所有者
            created_at: 创建时间（毫秒时间戳）
            created_by: 创建者
            updated_at: 更新时间（毫秒时间戳）
            updated_by: 更新者
        """
        super().__init__(owner, created_at, created_by, updated_at, updated_by)
        self.id = id
        self.name = name
        self.location = location
        self.options = options or {}

    def get_id(self) -> Optional[str]:
        """获取数据库 ID"""
        return self.id

    def get_name(self) -> Optional[str]:
        """获取数据库名称"""
        return self.name

    def get_location(self) -> Optional[str]:
        """获取数据库位置"""
        return self.location

    def get_options(self) -> Dict[str, str]:
        """获取数据库选项"""
        return self.options or {}

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            self.FIELD_ID: self.id,
            self.FIELD_NAME: self.name,
            self.FIELD_LOCATION: self.location,
            self.FIELD_OPTIONS: self.options
        }

        # 添加审计字段
        if self.owner is not None:
            result[self.FIELD_OWNER] = self.owner
        if self.created_at is not None:
            result[self.FIELD_CREATED_AT] = self.created_at
        if self.created_by is not None:
            result[self.FIELD_CREATED_BY] = self.created_by
        if self.updated_at is not None:
            result[self.FIELD_UPDATED_AT] = self.updated_at
        if self.updated_by is not None:
            result[self.FIELD_UPDATED_BY] = self.updated_by

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GetDatabaseResponse':
        """从字典创建实例"""
        return cls(
            id=data.get(cls.FIELD_ID),
            name=data.get(cls.FIELD_NAME),
            location=data.get(cls.FIELD_LOCATION),
            options=data.get(cls.FIELD_OPTIONS, {}),
            owner=data.get(cls.FIELD_OWNER),
            created_at=data.get(cls.FIELD_CREATED_AT),
            created_by=data.get(cls.FIELD_CREATED_BY),
            updated_at=data.get(cls.FIELD_UPDATED_AT),
            updated_by=data.get(cls.FIELD_UPDATED_BY)
        )

    def to_json(self) -> str:
        """转换为 JSON 字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> 'GetDatabaseResponse':
        """从 JSON 字符串创建实例"""
        data = json.loads(json_str)
        return cls.from_dict(data)

    def __str__(self) -> str:
        """字符串表示"""
        return f"GetDatabaseResponse(id={self.id}, name={self.name}, location={self.location})"

    def __repr__(self) -> str:
        """详细字符串表示"""
        return (f"GetDatabaseResponse(id={self.id!r}, name={self.name!r}, "
                f"location={self.location!r}, options={self.options!r}, "
                f"owner={self.owner!r}, created_at={self.created_at}, "
                f"created_by={self.created_by!r}, updated_at={self.updated_at}, "
                f"updated_by={self.updated_by!r})")


# JSON 序列化支持的增强版本
class JSONSerializableGetDatabaseResponse(GetDatabaseResponse):
    """支持 JSON 序列化的获取数据库响应"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __json__(self) -> Dict[str, Any]:
        """自定义 JSON 序列化"""
        return self.to_dict()

    @classmethod
    def __from_json__(cls, data: Dict[str, Any]) -> 'JSONSerializableGetDatabaseResponse':
        """自定义 JSON 反序列化"""
        return cls.from_dict(data)


# 构建器模式实现
class GetDatabaseResponseBuilder:
    """获取数据库响应构建器"""

    def __init__(self):
        self._id: Optional[str] = None
        self._name: Optional[str] = None
        self._location: Optional[str] = None
        self._options: Dict[str, str] = {}
        self._owner: Optional[str] = None
        self._created_at: Optional[int] = None
        self._created_by: Optional[str] = None
        self._updated_at: Optional[int] = None
        self._updated_by: Optional[str] = None

    def id(self, id: str) -> 'GetDatabaseResponseBuilder':
        """设置数据库 ID"""
        self._id = id
        return self

    def name(self, name: str) -> 'GetDatabaseResponseBuilder':
        """设置数据库名称"""
        self._name = name
        return self

    def location(self, location: str) -> 'GetDatabaseResponseBuilder':
        """设置数据库位置"""
        self._location = location
        return self

    def options(self, options: Dict[str, str]) -> 'GetDatabaseResponseBuilder':
        """设置数据库选项"""
        self._options = options or {}
        return self

    def add_option(self, key: str, value: str) -> 'GetDatabaseResponseBuilder':
        """添加单个选项"""
        self._options[key] = value
        return self

    def owner(self, owner: str) -> 'GetDatabaseResponseBuilder':
        """设置所有者"""
        self._owner = owner
        return self

    def created_at(self, created_at: int) -> 'GetDatabaseResponseBuilder':
        """设置创建时间"""
        self._created_at = created_at
        return self

    def created_by(self, created_by: str) -> 'GetDatabaseResponseBuilder':
        """设置创建者"""
        self._created_by = created_by
        return self

    def updated_at(self, updated_at: int) -> 'GetDatabaseResponseBuilder':
        """设置更新时间"""
        self._updated_at = updated_at
        return self

    def updated_by(self, updated_by: str) -> 'GetDatabaseResponseBuilder':
        """设置更新者"""
        self._updated_by = updated_by
        return self

    def build(self) -> GetDatabaseResponse:
        """构建响应对象"""
        return GetDatabaseResponse(
            id=self._id,
            name=self._name,
            location=self._location,
            options=self._options,
            owner=self._owner,
            created_at=self._created_at,
            created_by=self._created_by,
            updated_at=self._updated_at,
            updated_by=self._updated_by
        )


# 使用示例
def example_usage():
    """使用示例"""

    print("=== 示例1: 基础使用 ===")

    # 创建响应对象
    response = GetDatabaseResponse(
        id="db_001",
        name="my_database",
        location="/data/databases/my_database",
        options={"charset": "utf8", "engine": "innodb"},
        owner="admin",
        created_at=1640995200000,  # 2022-01-01 00:00:00
        created_by="system",
        updated_at=1640995200000,
        updated_by="admin"
    )

    # 获取属性
    print(f"Database ID: {response.get_id()}")
    print(f"Database Name: {response.get_name()}")
    print(f"Database Location: {response.get_location()}")
    print(f"Database Options: {response.get_options()}")
    print(f"Owner: {response.get_owner()}")
    print(f"Created At: {response.get_created_datetime()}")
    print(f"Updated At: {response.get_updated_datetime()}")

    print("\n" + "="*50 + "\n")

    # 示例2: 使用构建器模式
    print("=== 示例2: 构建器模式 ===")

    builder_response = (GetDatabaseResponseBuilder()
                        .id("db_002")
                        .name("test_database")
                        .location("/data/databases/test_database")
                        .add_option("charset", "utf8mb4")
                        .add_option("collation", "utf8mb4_unicode_ci")
                        .owner("test_user")
                        .created_at(1640995200000)
                        .created_by("test_system")
                        .build())

    print(f"Builder Response: {builder_response}")

    print("\n" + "="*50 + "\n")

    # 示例3: JSON 序列化和反序列化
    print("=== 示例3: JSON 序列化 ===")

    # 转换为 JSON
    json_str = response.to_json()
    print("JSON 字符串:")
    print(json_str)

    # 从 JSON 创建对象
    restored_response = GetDatabaseResponse.from_json(json_str)
    print(f"\n从 JSON 恢复的对象: {restored_response}")

    print("\n" + "="*50 + "\n")

    # 示例4: 字典转换
    print("=== 示例4: 字典转换 ===")

    # 转换为字典
    response_dict = response.to_dict()
    print("字典格式:")
    for key, value in response_dict.items():
        print(f"  {key}: {value}")

    # 从字典创建对象
    dict_response = GetDatabaseResponse.from_dict(response_dict)
    print(f"\n从字典创建的对象: {dict_response}")

    print("\n" + "="*50 + "\n")

    # 示例5: 处理空值
    print("=== 示例5: 处理空值 ===")

    minimal_response = GetDatabaseResponse(
        name="minimal_db"
        # 其他字段使用默认值
    )

    print(f"最小响应: {minimal_response}")
    print(f"选项: {minimal_response.get_options()}")
    print(f"创建时间: {minimal_response.get_created_datetime()}")


# 验证和测试工具
def validate_response(response: GetDatabaseResponse) -> bool:
    """验证响应对象"""
    errors = []

    # 检查必需字段
    if not response.get_name():
        errors.append("Database name is required")

    # 检查选项格式
    options = response.get_options()
    if options and not isinstance(options, dict):
        errors.append("Options must be a dictionary")

    # 检查时间戳
    if response.created_at and response.created_at <= 0:
        errors.append("Created at must be a positive timestamp")

    if response.updated_at and response.updated_at <= 0:
        errors.append("Updated at must be a positive timestamp")

    if errors:
        print(f"Validation errors: {errors}")
        return False

    return True


def test_serialization():
    """测试序列化性能"""
    import time

    response = GetDatabaseResponse(
        id="test_db",
        name="performance_test",
        location="/test/location",
        options={"key1": "value1", "key2": "value2"}
    )

    # 测试 JSON 序列化性能
    start_time = time.time()
    iterations = 10000

    for _ in range(iterations):
        json_str = response.to_json()
        GetDatabaseResponse.from_json(json_str)

    end_time = time.time()
    duration = end_time - start_time

    print(f"Serialization test: {iterations} iterations in {duration:.4f} seconds")
    print(f"Average time per serialization: {(duration / iterations) * 1000:.4f} ms")


if __name__ == "__main__":
    example_usage()
    print("\n" + "="*50 + "\n")
    test_serialization()
