---
title: "PyJindoSDK Support"
weight: 8
type: docs
aliases:
  - /pypaimon/pyjindosdk-support.html
---

# PyJindoSDK Support

## Introduction

[JindoSDK](https://github.com/aliyun/alibabacloud-jindodata) is a high-performance storage SDK developed by Alibaba Cloud for accessing OSS (Object Storage Service) and other cloud storage systems. It provides optimized I/O performance and deep integration with the Alibaba Cloud ecosystem.

PyPaimon now supports using [PyJindoSDK](https://github.com/aliyun/alibabacloud-jindodata) (the Python binding of JindoSDK) to access OSS. Compared to the legacy implementation based on PyArrow's S3FileSystem, PyJindoSDK offers better performance and compatibility when working with OSS.

## Usage

### Installation

Install `pyjindosdk` via pip:

```shell
pip install pyjindosdk
```

Once installed, PyPaimon will automatically use PyJindoSDK as the default file I/O implementation for accessing OSS. No additional configuration is required.

### Fallback to Legacy Implementation

Since JindoSDK is a native implementation, pre-built Python packages may not be available for all OS or platform versions. If you need to fall back to the legacy PyArrow-based implementation for any reason, there are two ways to do so:

**Option 1: Set catalog option `fs.oss.impl` to `legacy`**

```python
from pypaimon import CatalogFactory

catalog_options = {
    'metastore': 'rest',
    'uri': 'http://rest-server:8080',
    'warehouse': 'oss://my-bucket/warehouse',

    # Fallback to the legacy PyArrow S3FileSystem implementation
    'fs.oss.impl': 'legacy',
}

catalog = CatalogFactory.create(catalog_options)
```

**Option 2: Uninstall pyjindosdk**

Simply uninstalling the `pyjindosdk` package will cause PyPaimon to automatically fall back to the legacy implementation:

```shell
pip uninstall pyjindosdk
```
