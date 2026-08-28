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
from setuptools.command.build_py import build_py
from setuptools.command.sdist import sdist

PYTHON_ROOT = os.path.dirname(os.path.abspath(__file__))
FULL_VERSION_FILE = os.path.join(PYTHON_ROOT, "pypaimon", "_full_version")
UNKNOWN_COMMIT_ID = "UNKNOWN"

VERSION = "2.1.dev"


def _repository_root():
    parent = os.path.dirname(PYTHON_ROOT)
    if os.path.basename(PYTHON_ROOT) == "paimon-python" and os.path.exists(
            os.path.join(parent, "pom.xml")):
        return parent
    return PYTHON_ROOT


def _git_output(args):
    repository_root = _repository_root()
    env = os.environ.copy()
    env["GIT_CEILING_DIRECTORIES"] = os.path.dirname(repository_root)
    try:
        return subprocess.check_output(
            ["git", "-C", repository_root] + args,
            stderr=subprocess.DEVNULL,
            env=env,
        ).decode("utf-8").strip()
    except Exception:
        return None


def _full_version():
    try:
        with open(FULL_VERSION_FILE, "r") as full_version_file:
            embedded = full_version_file.read().strip()
            if embedded:
                return embedded
    except OSError:
        pass

    git_commit_id = _git_output(["rev-parse", "HEAD"])
    if git_commit_id is None:
        git_commit_id = UNKNOWN_COMMIT_ID
    return "python-{}-{}".format(VERSION, git_commit_id)


def _write_full_version(root):
    package_dir = os.path.join(root, "pypaimon")
    if not os.path.exists(package_dir):
        os.makedirs(package_dir)
    with open(os.path.join(package_dir, "_full_version"), "w") as full_version_file:
        full_version_file.write(_full_version() + "\n")


class PaimonBuildPy(build_py):

    def run(self):
        build_py.run(self)
        _write_full_version(self.build_lib)


class PaimonSdist(sdist):

    def make_release_tree(self, base_dir, files):
        sdist.make_release_tree(self, base_dir, files)
        _write_full_version(base_dir)


def get_dev_version():
    """Generate dev version with commit date.
    Format: 2.1.devYYYYMMDD (e.g. 2.1.dev20260415)
    Uses the commit date (author date) for reproducibility.
    """
    base = VERSION.rstrip(".")
    if not base.endswith("dev"):
        return None

    try:
        date_str = _git_output(
            ["log", "-1", "--format=%cd", "--date=format:%Y%m%d"])
        if date_str is None:
            raise RuntimeError("Git commit date is unavailable")
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

        # Keep the embedded full version consistent with the dev package version.
        full_version_file = os.path.join(dev_dir, "pypaimon", "_full_version")
        if os.path.exists(full_version_file):
            with open(full_version_file, "r") as f:
                content = f.read()
            version_prefix = "python-{}-".format(VERSION)
            if content.startswith(version_prefix):
                content = "python-{}-{}".format(
                    dev_version, content[len(version_prefix):])
            with open(full_version_file, "w") as f:
                f.write(content)

        dev_tar = os.path.join("dist", dev_name + ".tar.gz")
        with tarfile.open(dev_tar, "w:gz") as tar:
            tar.add(dev_dir, arcname=dev_name)

        print("Created dev package: " + dev_tar)
    finally:
        shutil.rmtree(tmp_dir)


atexit.register(_build_dev_package)

PACKAGES = find_packages(
    include=["pypaimon*"],
    exclude=["pypaimon.tests*", "pypaimon.acceptance*"],
)


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
    package_data={"pypaimon": ["_full_version"]},
    cmdclass={"build_py": PaimonBuildPy, "sdist": PaimonSdist},
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'paimon=pypaimon.cli:main',
        ],
    },
    extras_require={
        'hdf5': [
            # HDF5 loading is explicitly guarded and documented as Python 3.8+.
            'h5py>=3,<4; python_version>="3.8"',
        ],
        'lerobot': [
            # datasets 4.1+ may select PyArrow 21+, while PyPaimon currently
            # supports PyArrow <20. LeRobot 0.4.x uses the v3 dataset format.
            'datasets>=4,<4.1; python_version>="3.10"',
            'lerobot>=0.4.4,<0.5; python_version>="3.10"',
        ],
        'ray': [
            'ray>=2.10,<3; python_version>="3.8"',
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
        'jindo': [
            'pyjindosdk>=6.10.4',
        ],
        'lance': [
            'pylance>=0.20,<1; python_version>="3.9"',
            'pylance>=0.10,<1; python_version>="3.8" and python_version<"3.9"'
        ],
        'vortex': [
            'vortex-data==0.70.0; python_version>="3.11"',
        ],
        'mosaic': [
            'paimon-mosaic>=0.1.0; python_version>="3.9"',
        ],
        'lumina': [
            'lumina-data>=0.1.0'
        ],
        'vindex': [
            'paimon-vindex==0.4.0; python_version>="3.9"',
        ],
        'full-text': [
            'paimon-ftindex==0.1.0; python_version>="3.8"',
        ],
        'theta-sketch': [
            'datasketches>=4,<5; python_version<"3.9"',
            'datasketches>=5,<6; python_version>="3.9"',
        ],
        'hll-sketch': [
            'datasketches>=4,<5; python_version<"3.9"',
            'datasketches>=5,<6; python_version>="3.9"',
        ],
        'sql': [
            'pypaimon-rust>=0.3.0; python_version>="3.10"',
            'datafusion>=54,<55; python_version>="3.10"',
        ],
        'hdfs': [
            'hdfs-native>=0.13,<1; python_version >= "3.10" and platform_system != "Windows"',
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
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    python_requires=">=3.6",
)
