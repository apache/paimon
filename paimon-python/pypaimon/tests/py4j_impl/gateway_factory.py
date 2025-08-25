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

import os
import shutil
import struct
import tempfile
import time
from logging import WARN
from threading import RLock

from py4j.java_gateway import (CallbackServerParameters, GatewayParameters,
                               JavaGateway, JavaPackage, java_import, logger)

from pypaimon.tests.py4j_impl import constants
from pypaimon.tests.py4j_impl.gateway_server import \
    launch_gateway_server_process

_gateway = None
_lock = RLock()


def get_gateway():
    # type: () -> JavaGateway
    global _gateway
    with _lock:
        if _gateway is None:
            # Set the level to WARN to mute the noisy INFO level logs
            logger.level = WARN
            _gateway = launch_gateway()

            callback_server = _gateway.get_callback_server()
            callback_server_listening_address = callback_server.get_listening_address()
            callback_server_listening_port = callback_server.get_listening_port()
            _gateway.jvm.org.apache.paimon.python.PythonEnvUtils.resetCallbackClient(
                _gateway.java_gateway_server,
                callback_server_listening_address,
                callback_server_listening_port)
            import_paimon_view(_gateway)
            install_py4j_hooks()
            _gateway.entry_point.put("Watchdog", Watchdog())
    return _gateway


def launch_gateway():
    # type: () -> JavaGateway
    """
    launch jvm gateway
    """

    # Create a temporary directory where the gateway server should write the connection information.
    conn_info_dir = tempfile.mkdtemp()
    try:
        fd, conn_info_file = tempfile.mkstemp(dir=conn_info_dir)
        os.close(fd)
        os.unlink(conn_info_file)

        env = dict(os.environ)
        env[constants.PYPAIMON_CONN_INFO_PATH] = conn_info_file

        p = launch_gateway_server_process(env)

        while not p.poll() and not os.path.isfile(conn_info_file):
            time.sleep(0.1)

        if not os.path.isfile(conn_info_file):
            stderr_info = p.stderr.read().decode('utf-8')
            raise RuntimeError(
                "Java gateway process exited before sending its port number.\nStderr:\n"
                + stderr_info
            )

        with open(conn_info_file, "rb") as info:
            gateway_port = struct.unpack("!I", info.read(4))[0]
    finally:
        shutil.rmtree(conn_info_dir)

    # Connect to the gateway
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(port=gateway_port, auto_convert=True),
        callback_server_parameters=CallbackServerParameters(
            port=0, daemonize=True, daemonize_connections=True))

    return gateway


def import_paimon_view(gateway):
    java_import(gateway.jvm, "org.apache.paimon.table.*")
    java_import(gateway.jvm, "org.apache.paimon.options.Options")
    java_import(gateway.jvm, "org.apache.paimon.catalog.*")
    java_import(gateway.jvm, "org.apache.paimon.schema.Schema*")
    java_import(gateway.jvm, 'org.apache.paimon.types.*')
    java_import(gateway.jvm, 'org.apache.paimon.python.*')
    java_import(gateway.jvm, "org.apache.paimon.data.*")
    java_import(gateway.jvm, "org.apache.paimon.predicate.PredicateBuilder")


def install_py4j_hooks():
    """
    Hook the classes such as JavaPackage, etc of Py4j to improve the exception message.
    """
    def wrapped_call(self, *args, **kwargs):
        raise TypeError(
            "Could not found the Java class '%s'. The Java dependencies could be specified via "
            "command line argument '--jarfile' or the config option 'pipeline.jars'" % self._fqn)

    setattr(JavaPackage, '__call__', wrapped_call)


class Watchdog(object):
    """
    Used to provide to Java side to check whether its parent process is alive.
    """

    def ping(self):
        time.sleep(10)
        return True

    class Java:
        implements = ["org.apache.paimon.python.PythonGatewayServer$Watchdog"]
