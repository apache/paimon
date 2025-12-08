#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
监控和诊断模块。

提供性能监控、数据采样预览和错误重试机制。
"""

import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)


class PerformanceMonitor:
    """性能监控器。

    跟踪关键操作的性能指标（吞吐量、延迟、错误率）。

    Example:
        >>> monitor = PerformanceMonitor()
        >>> monitor.start_timer('data_loading')
        >>> # 执行数据加载操作
        >>> monitor.end_timer('data_loading')
        >>> metrics = monitor.get_metrics()
    """

    def __init__(self):
        """初始化性能监控器。"""
        self.timers: Dict[str, float] = {}
        self.metrics: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {'count': 0, 'total_time': 0, 'errors': 0}
        )
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def start_timer(self, operation_name: str) -> None:
        """启动计时器。

        Args:
            operation_name: 操作名称
        """
        self.timers[operation_name] = time.time()
        self.logger.debug(f"启动计时器：{operation_name}")

    def end_timer(self, operation_name: str) -> float:
        """结束计时器并记录时间。

        Args:
            operation_name: 操作名称

        Returns:
            耗时（秒）
        """
        try:
            if operation_name not in self.timers:
                self.logger.warning(f"计时器未启动：{operation_name}")
                return 0

            elapsed = time.time() - self.timers[operation_name]
            del self.timers[operation_name]

            self.metrics[operation_name]['count'] += 1
            self.metrics[operation_name]['total_time'] += elapsed

            self.logger.debug(f"计时器结束：{operation_name}，耗时={elapsed:.3f}s")

            return elapsed

        except Exception as e:
            self.logger.error(f"计时器结束失败：{e}")
            return 0

    def record_error(self, operation_name: str) -> None:
        """记录错误。

        Args:
            operation_name: 操作名称
        """
        if operation_name not in self.metrics:
            self.metrics[operation_name]['count'] = 1

        self.metrics[operation_name]['errors'] += 1
        self.logger.debug(f"记录错误：{operation_name}")

    def get_metrics(self) -> Dict[str, Dict[str, float]]:
        """获取所有性能指标。

        Returns:
            性能指标字典

        Example:
            >>> metrics = monitor.get_metrics()
            >>> print(metrics['data_loading'])
            >>> # {
            >>> #     'count': 100,
            >>> #     'total_time': 15.5,
            >>> #     'avg_time': 0.155,
            >>> #     'throughput': 6.45,
            >>> #     'error_rate': 0.0
            >>> # }
        """
        result = {}

        for op_name, stats in self.metrics.items():
            count = stats['count']
            total_time = stats['total_time']
            errors = stats['errors']

            avg_time = total_time / count if count > 0 else 0
            throughput = count / total_time if total_time > 0 else 0
            error_rate = errors / count if count > 0 else 0

            result[op_name] = {
                'count': count,
                'total_time': total_time,
                'avg_time': avg_time,
                'throughput': throughput,
                'error_rate': error_rate,
                'errors': errors,
            }

        return result

    def print_report(self) -> None:
        """打印性能报告。"""
        metrics = self.get_metrics()

        print("\n" + "="*80)
        print("性能监控报告")
        print("="*80)

        for op_name, stats in metrics.items():
            print(f"\n{op_name}:")
            print(f"  调用次数：{stats['count']}")
            print(f"  总耗时：{stats['total_time']:.3f}s")
            print(f"  平均耗时：{stats['avg_time']:.3f}s")
            print(f"  吞吐量：{stats['throughput']:.2f} ops/sec")
            print(f"  错误率：{stats['error_rate']*100:.2f}%")

        print("\n" + "="*80)


class DataSampler:
    """数据采样预览器。

    从大型数据集中采样数据进行预览和验证。

    Example:
        >>> sampler = DataSampler(sample_size=100)
        >>> samples = sampler.sample(dataset)
    """

    def __init__(self, sample_size: int = 100, random_seed: Optional[int] = None):
        """初始化数据采样器。

        Args:
            sample_size: 采样大小
            random_seed: 随机种子
        """
        self.sample_size = sample_size
        self.random_seed = random_seed
        self.samples: List[Any] = []

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def sample(self, data_iterator: Any) -> List[Any]:
        """从迭代器采样数据。

        Args:
            data_iterator: 数据迭代器

        Returns:
            采样数据列表

        Example:
            >>> dataset = [i for i in range(1000)]
            >>> sampler = DataSampler(sample_size=10)
            >>> samples = sampler.sample(iter(dataset))
        """
        try:
            self.samples = []
            count = 0

            for data in data_iterator:
                count += 1

                # 使用储水库采样算法
                if len(self.samples) < self.sample_size:
                    self.samples.append(data)
                else:
                    import random
                    j = random.randint(0, count - 1)
                    if j < self.sample_size:
                        self.samples[j] = data

            self.logger.info(
                f"采样完成：从 {count} 条数据中采样 {len(self.samples)} 条"
            )

            return self.samples

        except Exception as e:
            self.logger.error(f"数据采样失败：{e}", exc_info=True)
            return []

    def get_statistics(self) -> Dict[str, Any]:
        """获取采样数据的统计信息。

        Returns:
            统计信息字典
        """
        if not self.samples:
            return {'sample_count': 0}

        try:
            return {
                'sample_count': len(self.samples),
                'first_sample': self.samples[0],
                'last_sample': self.samples[-1],
                'sample_types': [type(s).__name__ for s in self.samples[:5]],
            }

        except Exception as e:
            self.logger.error(f"统计信息获取失败：{e}")
            return {'sample_count': len(self.samples)}


class RetryPolicy:
    """重试策略。

    定义错误处理的重试行为（指数退避、最大重试次数等）。

    Example:
        >>> policy = RetryPolicy(max_retries=3, initial_delay=1.0)
        >>> result = policy.execute(risky_function, arg1, arg2)
    """

    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        retry_on_exceptions: Optional[tuple] = None
    ):
        """初始化重试策略。

        Args:
            max_retries: 最大重试次数
            initial_delay: 初始延迟（秒）
            max_delay: 最大延迟（秒）
            exponential_base: 指数退避基数
            retry_on_exceptions: 需要重试的异常类型
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.retry_on_exceptions = retry_on_exceptions or (Exception,)

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """执行函数，带重试逻辑。

        Args:
            func: 要执行的函数
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            函数返回值

        Example:
            >>> def fetch_data():
            ...     import random
            ...     if random.random() < 0.7:
            ...         raise ConnectionError()
            ...     return 'data'
            >>>
            >>> policy = RetryPolicy(max_retries=3)
            >>> result = policy.execute(fetch_data)
        """
        last_exception: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    self.logger.info(
                        f"执行成功（第 {attempt + 1} 次尝试）"
                    )
                return result

            except self.retry_on_exceptions as e:
                last_exception = e

                if attempt >= self.max_retries:
                    self.logger.error(
                        f"执行失败，已达最大重试次数：{self.max_retries}"
                    )
                    raise

                # 计算延迟
                delay = min(
                    self.initial_delay * (self.exponential_base ** attempt),
                    self.max_delay
                )

                self.logger.warning(
                    f"执行失败（尝试 {attempt + 1}/{self.max_retries + 1}），"
                    f"{delay:.2f}s 后重试：{str(e)}"
                )

                time.sleep(delay)

            except Exception as e:
                self.logger.error(f"执行失败，不进行重试：{str(e)}")
                raise

        if last_exception:
            raise last_exception
        raise RuntimeError("执行失败")


class DataValidator:
    """数据验证器。

    验证数据的有效性和完整性。

    Example:
        >>> validator = DataValidator()
        >>> validator.add_rule('age', lambda x: 0 <= x <= 150)
        >>> is_valid = validator.validate({'age': 25})
    """

    def __init__(self):
        """初始化数据验证器。"""
        self.rules: Dict[str, List[Tuple[Callable, Optional[str]]]] = defaultdict(list)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def add_rule(
        self,
        field: str,
        rule: Callable,
        error_message: Optional[str] = None
    ) -> 'DataValidator':
        """添加验证规则。

        Args:
            field: 字段名
            rule: 验证函数（返回 True/False）
            error_message: 错误消息

        Returns:
            self（用于链式调用）

        Example:
            >>> validator = DataValidator()
            >>> validator.add_rule('email', lambda x: '@' in x, '无效的邮箱')
            >>> validator.add_rule('age', lambda x: x > 0, '年龄必须大于 0')
        """
        self.rules[field].append((rule, error_message))
        return self

    def validate(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """验证数据。

        Args:
            data: 要验证的数据字典

        Returns:
            (验证结果, 错误消息列表)

        Example:
            >>> validator = DataValidator()
            >>> validator.add_rule('name', lambda x: len(x) > 0)
            >>> is_valid, errors = validator.validate({'name': 'John'})
        """
        errors = []

        for field, field_rules in self.rules.items():
            if field not in data:
                errors.append(f"字段缺失：{field}")
                continue

            value = data[field]

            for rule, error_message in field_rules:
                try:
                    if not rule(value):
                        error = error_message or f"字段验证失败：{field}"
                        errors.append(error)

                except Exception as e:
                    errors.append(f"字段验证异常：{field}，{str(e)}")

        is_valid = len(errors) == 0

        if not is_valid:
            self.logger.warning(f"数据验证失败：{errors}")
        else:
            self.logger.debug("数据验证通过")

        return is_valid, errors
