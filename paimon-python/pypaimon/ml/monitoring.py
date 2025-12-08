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
Monitoring and diagnostics module.

Provides performance monitoring, data sampling preview, and error retry mechanisms.
"""

import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)


class PerformanceMonitor:
    """Performance monitor.

    Tracks performance metrics (throughput, latency, error rate) of critical operations.

    Example:
        >>> monitor = PerformanceMonitor()
        >>> monitor.start_timer('data_loading')
        >>> # Perform data loading operation
        >>> monitor.end_timer('data_loading')
        >>> metrics = monitor.get_metrics()
    """

    def __init__(self):
        """Initialize the performance monitor."""
        self.timers: Dict[str, float] = {}
        self.metrics: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {'count': 0, 'total_time': 0, 'errors': 0}
        )
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def start_timer(self, operation_name: str) -> None:
        """Start timer.

        Args:
            operation_name: Operation name
        """
        self.timers[operation_name] = time.time()
        self.logger.debug(f"Timer started: {operation_name}")

    def end_timer(self, operation_name: str) -> float:
        """End timer and record elapsed time.

        Args:
            operation_name: Operation name

        Returns:
            Elapsed time (seconds)
        """
        try:
            if operation_name not in self.timers:
                self.logger.warning(f"Timer not started: {operation_name}")
                return 0

            elapsed = time.time() - self.timers[operation_name]
            del self.timers[operation_name]

            self.metrics[operation_name]['count'] += 1
            self.metrics[operation_name]['total_time'] += elapsed

            self.logger.debug(f"Timer ended: {operation_name}, elapsed={elapsed:.3f}s")

            return elapsed

        except Exception as e:
            self.logger.error(f"Timer end failed: {e}")
            return 0

    def record_error(self, operation_name: str) -> None:
        """Record error.

        Args:
            operation_name: Operation name
        """
        if operation_name not in self.metrics:
            self.metrics[operation_name]['count'] = 1

        self.metrics[operation_name]['errors'] += 1
        self.logger.debug(f"Error recorded: {operation_name}")

    def get_metrics(self) -> Dict[str, Dict[str, float]]:
        """Get all performance metrics.

        Returns:
            Performance metrics dictionary

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
        """Print performance report."""
        metrics = self.get_metrics()

        print("\n" + "="*80)
        print("Performance Monitoring Report")
        print("="*80)

        for op_name, stats in metrics.items():
            print(f"\n{op_name}:")
            print(f"  Invocation count: {stats['count']}")
            print(f"  Total time: {stats['total_time']:.3f}s")
            print(f"  Average time: {stats['avg_time']:.3f}s")
            print(f"  Throughput: {stats['throughput']:.2f} ops/sec")
            print(f"  Error rate: {stats['error_rate']*100:.2f}%")

        print("\n" + "="*80)


class DataSampler:
    """Data sampling previewer.

    Sample data from large datasets for preview and validation.

    Example:
        >>> sampler = DataSampler(sample_size=100)
        >>> samples = sampler.sample(dataset)
    """

    def __init__(self, sample_size: int = 100, random_seed: Optional[int] = None):
        """Initialize data sampler.

        Args:
            sample_size: Sample size
            random_seed: Random seed
        """
        self.sample_size = sample_size
        self.random_seed = random_seed
        self.samples: List[Any] = []

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def sample(self, data_iterator: Any) -> List[Any]:
        """Sample data from iterator.

        Args:
            data_iterator: Data iterator

        Returns:
            List of sampled data

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

                # Use reservoir sampling algorithm
                if len(self.samples) < self.sample_size:
                    self.samples.append(data)
                else:
                    import random
                    j = random.randint(0, count - 1)
                    if j < self.sample_size:
                        self.samples[j] = data

            self.logger.info(
                f"Sampling completed: sampled {len(self.samples)} items from {count} items"
            )

            return self.samples

        except Exception as e:
            self.logger.error(f"Data sampling failed: {e}", exc_info=True)
            return []

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics of sampled data.

        Returns:
            Statistics dictionary
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
            self.logger.error(f"Statistics retrieval failed: {e}")
            return {'sample_count': len(self.samples)}


class RetryPolicy:
    """Retry policy.

    Defines retry behavior for error handling (exponential backoff, max retries, etc.).

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
        """Initialize retry policy.

        Args:
            max_retries: Maximum number of retries
            initial_delay: Initial delay (seconds)
            max_delay: Maximum delay (seconds)
            exponential_base: Exponential backoff base
            retry_on_exceptions: Exception types to retry on
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.retry_on_exceptions = retry_on_exceptions or (Exception,)

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic.

        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Function return value

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
                        f"Execution successful (attempt {attempt + 1})"
                    )
                return result

            except self.retry_on_exceptions as e:
                last_exception = e

                if attempt >= self.max_retries:
                    self.logger.error(
                        f"Execution failed, maximum retries reached: {self.max_retries}"
                    )
                    raise

                # Calculate delay
                delay = min(
                    self.initial_delay * (self.exponential_base ** attempt),
                    self.max_delay
                )

                self.logger.warning(
                    f"Execution failed (attempt {attempt + 1}/{self.max_retries + 1}), "
                    f"retrying in {delay:.2f}s: {str(e)}"
                )

                time.sleep(delay)

            except Exception as e:
                self.logger.error(f"Execution failed, no retry: {str(e)}")
                raise

        if last_exception:
            raise last_exception
        raise RuntimeError("Execution failed")


class DataValidator:
    """Data validator.

    Validate data validity and completeness.

    Example:
        >>> validator = DataValidator()
        >>> validator.add_rule('age', lambda x: 0 <= x <= 150)
        >>> is_valid = validator.validate({'age': 25})
    """

    def __init__(self):
        """Initialize data validator."""
        self.rules: Dict[str, List[Tuple[Callable, Optional[str]]]] = defaultdict(list)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def add_rule(
        self,
        field: str,
        rule: Callable,
        error_message: Optional[str] = None
    ) -> 'DataValidator':
        """Add validation rule.

        Args:
            field: Field name
            rule: Validation function (returns True/False)
            error_message: Error message

        Returns:
            self (for chaining)

        Example:
            >>> validator = DataValidator()
            >>> validator.add_rule('email', lambda x: '@' in x, 'Invalid email')
            >>> validator.add_rule('age', lambda x: x > 0, 'Age must be greater than 0')
        """
        self.rules[field].append((rule, error_message))
        return self

    def validate(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate data.

        Args:
            data: Data dictionary to validate

        Returns:
            (validation result, error message list)

        Example:
            >>> validator = DataValidator()
            >>> validator.add_rule('name', lambda x: len(x) > 0)
            >>> is_valid, errors = validator.validate({'name': 'John'})
        """
        errors = []

        for field, field_rules in self.rules.items():
            if field not in data:
                errors.append(f"Field missing: {field}")
                continue

            value = data[field]

            for rule, error_message in field_rules:
                try:
                    if not rule(value):
                        error = error_message or f"Field validation failed: {field}"
                        errors.append(error)

                except Exception as e:
                    errors.append(f"Field validation exception: {field}, {str(e)}")

        is_valid = len(errors) == 0

        if not is_valid:
            self.logger.warning(f"Data validation failed: {errors}")
        else:
            self.logger.debug("Data validation passed")

        return is_valid, errors
