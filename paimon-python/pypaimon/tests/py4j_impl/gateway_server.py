################################################################################
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
################################################################################

import importlib.resources
import os
import platform
import signal
from subprocess import PIPE, Popen

from pypaimon.tests.py4j_impl import constants


def on_windows():
    return platform.system() == "Windows"


def find_java_executable():
    java_executable = "java.exe" if on_windows() else "java"
    java_home = None

    if java_home is None and "JAVA_HOME" in os.environ:
        java_home = os.environ["JAVA_HOME"]

    if java_home is not None:
        java_executable = os.path.join(java_home, "bin", java_executable)

    return java_executable


def launch_gateway_server_process(env):
    java_executable = find_java_executable()
    log_settings = []
    jvm_args = env.get(constants.PYPAIMON_JVM_ARGS, '').split()
    classpath = _get_classpath(env)
    main_args = env.get(constants.PYPAIMON_MAIN_ARGS, '').split()
    command = [
        java_executable,
        *jvm_args,
        # default jvm args
        "-XX:+IgnoreUnrecognizedVMOptions",
        "--add-opens=jdk.proxy2/jdk.proxy2=ALL-UNNAMED",
        *log_settings,
        "-cp",
        classpath,
        "-c",
        constants.PYPAIMON_MAIN_CLASS,
        *main_args
    ]

    preexec_fn = None
    if not on_windows():
        def preexec_func():
            # ignore ctrl-c / SIGINT
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        preexec_fn = preexec_func
    return Popen(list(filter(lambda c: len(c) != 0, command)),
                 stdin=PIPE, stderr=PIPE, preexec_fn=preexec_fn, env=env)


_JAVA_DEPS_PACKAGE = 'pypaimon.jars'


def _get_classpath(env):
    classpath = []

    # note that jars are not packaged in test
    test_mode = os.environ.get(constants.PYPAIMON4J_TEST_MODE)
    if not test_mode or test_mode.lower() != "true":
        jars = importlib.resources.files(_JAVA_DEPS_PACKAGE)
        one_jar = next(iter(jars.iterdir()), None)
        if not one_jar:
            raise ValueError("Haven't found necessary python-java-bridge jar, this is unexpected.")
        builtin_java_classpath = os.path.join(os.path.dirname(str(one_jar)), '*')
        classpath.append(builtin_java_classpath)

    # user defined
    if constants.PYPAIMON_JAVA_CLASSPATH in env:
        classpath.append(env[constants.PYPAIMON_JAVA_CLASSPATH])

    # hadoop
    hadoop_classpath = _get_hadoop_classpath(env)
    if hadoop_classpath is not None:
        classpath.append(hadoop_classpath)

    return os.pathsep.join(classpath)


_HADOOP_DEPS_PACKAGE = 'pypaimon.hadoop-deps'


def _get_hadoop_classpath(env):
    if constants.PYPAIMON_HADOOP_CLASSPATH in env:
        return env[constants.PYPAIMON_HADOOP_CLASSPATH]
    elif 'HADOOP_CLASSPATH' in env:
        return env['HADOOP_CLASSPATH']
    else:
        # use built-in hadoop
        jars = importlib.resources.files(_HADOOP_DEPS_PACKAGE)
        one_jar = next(iter(jars.iterdir()), None)
        if not one_jar:
            raise EnvironmentError(f"The built-in Hadoop environment has been broken, this \
            is unexpected. You can set one of '{constants.PYPAIMON_HADOOP_CLASSPATH}' or \
            'HADOOP_CLASSPATH' to continue.")
        return os.path.join(os.path.dirname(str(one_jar)), '*')
