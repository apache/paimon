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
"""Setuptools hooks and the Python 3.6 bridge for pyproject.toml metadata."""
import os
import runpy
import sys

from packaging.version import Version
from setuptools import find_packages, setup
from setuptools.command.build_py import build_py
from setuptools.command.sdist import sdist

PYTHON_ROOT = os.path.dirname(os.path.abspath(__file__))
BUILD_INFO = runpy.run_path(os.path.join(PYTHON_ROOT, "pypaimon", "build_info.py"))
VERSION = str(Version(os.environ.get("PYPAIMON_BUILD_VERSION") or
                      BUILD_INFO["package_version"]()))
COMMIT_ID = BUILD_INFO["full_version"]().rsplit("-", 1)[-1]
FULL_VERSION = "python-{}-{}".format(VERSION, COMMIT_ID)


def _write_full_version(root):
    package_dir = os.path.join(root, "pypaimon")
    os.makedirs(package_dir, exist_ok=True)
    with open(os.path.join(package_dir, "_full_version"), "w") as version_file:
        version_file.write(FULL_VERSION + "\n")


class PaimonBuildPy(build_py):

    def run(self):
        build_py.run(self)
        _write_full_version(self.build_lib)


class PaimonSdist(sdist):

    def make_release_tree(self, base_dir, files):
        sdist.make_release_tree(self, base_dir, files)
        _write_full_version(base_dir)


def _legacy_metadata():
    # Python 3.6's last setuptools does not support PEP 621. Translate the same
    # TOML metadata instead of keeping another dependency list or dropping 3.6.
    import tomli

    with open(os.path.join(PYTHON_ROOT, "pyproject.toml"), "rb") as project_file:
        config = tomli.load(project_file)
    project = config["project"]
    setuptools_config = config["tool"]["setuptools"]
    with open(os.path.join(PYTHON_ROOT, project["readme"]), encoding="utf-8") as readme:
        long_description = readme.read()
    author = project["authors"][0]
    return dict(
        name=project["name"],
        description=project["description"],
        long_description=long_description,
        long_description_content_type="text/markdown",
        license=project["license"]["text"],
        author=author["name"],
        author_email=author["email"],
        url=project["urls"]["Homepage"],
        project_urls=project["urls"],
        python_requires=project["requires-python"],
        classifiers=project["classifiers"],
        install_requires=project["dependencies"],
        extras_require=project["optional-dependencies"],
        entry_points={"console_scripts": [
            "{}={}".format(name, entry) for name, entry in project["scripts"].items()
        ]},
        packages=find_packages(**setuptools_config["packages"]["find"]),
        include_package_data=setuptools_config["include-package-data"],
        package_data=setuptools_config["package-data"],
        license_files=setuptools_config["license-files"],
    )


setup(
    version=VERSION,
    cmdclass={"build_py": PaimonBuildPy, "sdist": PaimonSdist},
    **(_legacy_metadata() if sys.version_info < (3, 7) else {})
)
