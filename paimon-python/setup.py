##########################################################################
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
##########################################################################
import atexit
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from setuptools import find_packages, setup

VERSION = "1.5.dev"


def get_dev_version():
    """Generate dev version with commit date.
    Format: 1.5.devYYYYMMDD (e.g. 1.5.dev20260415)
    Uses the commit date (author date) for reproducibility.
    """
    base = VERSION.rstrip(".")
    if not base.endswith("dev"):
        return None

    try:
        date_str = subprocess.check_output(
            ["git", "log", "-1", "--format=%cd", "--date=format:%Y%m%d"],
            stderr=subprocess.DEVNULL
        ).decode("utf-8").strip()
    except Exception:
        print("Warning: git not available, skipping dev package.")
        return None

    return base + date_str


def _build_dev_package():
    """After sdist completes, repack a copy with dev version."""
    if "sdist" not in sys.argv:
        return

    dev_version = get_dev_version()
    if dev_version is None:
        return

    from packaging.version import Version
    normalized = str(Version(VERSION))
    dev_normalized = str(Version(dev_version))

    src_name = "pypaimon-{}".format(normalized)
    dev_name = "pypaimon-{}".format(dev_normalized)

    src_tar = os.path.join("dist", src_name + ".tar.gz")
    if not os.path.exists(src_tar):
        return

    tmp_dir = tempfile.mkdtemp()
    try:
        with tarfile.open(src_tar, "r:gz") as tar:
            tar.extractall(tmp_dir)

        src_dir = os.path.join(tmp_dir, src_name)
        dev_dir = os.path.join(tmp_dir, dev_name)
        os.rename(src_dir, dev_dir)

        # Update version in PKG-INFO files
        for pkg_info in [
            os.path.join(dev_dir, "PKG-INFO"),
            os.path.join(dev_dir, "pypaimon.egg-info", "PKG-INFO"),
        ]:
            if os.path.exists(pkg_info):
                with open(pkg_info, "r") as f:
                    content = f.read()
                content = content.replace(
                    "Version: " + normalized,
                    "Version: " + dev_normalized
                )
                with open(pkg_info, "w") as f:
                    f.write(content)

        # Update VERSION in setup.py so pip install gets the correct version
        setup_py = os.path.join(dev_dir, "setup.py")
        if os.path.exists(setup_py):
            with open(setup_py, "r") as f:
                content = f.read()
            content = content.replace(
                'VERSION = "' + VERSION + '"',
                'VERSION = "' + dev_version + '"'
            )
            with open(setup_py, "w") as f:
                f.write(content)

        dev_tar = os.path.join("dist", dev_name + ".tar.gz")
        with tarfile.open(dev_tar, "w:gz") as tar:
            tar.add(dev_dir, arcname=dev_name)

        print("Created dev package: " + dev_tar)
    finally:
        shutil.rmtree(tmp_dir)


atexit.register(_build_dev_package)

PACKAGES = find_packages(include=["pypaimon*"])


def read_requirements():
    """Read requirements from dev/requirements.txt file."""
    requirements_path = os.path.join(os.path.dirname(__file__), 'dev', 'requirements.txt')
    requirements = []

    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    requirements.append(line)

    return requirements


install_requires = read_requirements()

long_description = "See Apache Paimon Python API \
[Doc](https://paimon.apache.org/docs/master/pypaimon/python-api/) for usage."

setup(
    name="pypaimon",
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'paimon=pypaimon.cli:main',
        ],
    },
    extras_require={
        'ray': [
            'ray>=2.10,<3; python_version>="3.7"',
        ],
        'torch': [
            'torch',
        ],
        'daft': [
            'daft>=0.7.6; python_version>="3.10"',
        ],
        'oss': [
            'ossfs>=2021.8; python_version<"3.8"',
            'ossfs>=2023; python_version>="3.8"'
        ],
        'lance': [
            'pylance>=0.20,<1; python_version>="3.9"',
            'pylance>=0.10,<1; python_version>="3.8" and python_version<"3.9"'
        ],
        'lumina': [
            'lumina-data>=0.1.0'
        ],
        'sql': [
            'pypaimon-rust; python_version>="3.10"',
            'datafusion>=52; python_version>="3.10"',
        ],
    },
    description="Apache Paimon Python API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Apache Software Foundation",
    author_email="dev@paimon.apache.org",
    url="https://paimon.apache.org",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.6",
)
