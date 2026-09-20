#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Regression test for the Maven staging repository deployment script."""

import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / 'releasing' / 'deploy_maven_repository.sh'


class DeployMavenRepositoryTest(unittest.TestCase):
    def test_deploy_runs_once_without_recursing_into_reactor(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            repository = root / 'repository'
            artifact = repository / 'org' / 'apache' / 'paimon' / 'paimon-test.pom'
            artifact.parent.mkdir(parents=True)
            artifact.write_text('<project/>')
            signature = Path(str(artifact) + '.asc')
            signature.write_text('signature')
            for path in (artifact, signature):
                Path(str(path) + '.md5').write_text(hashlib.md5(path.read_bytes()).hexdigest())
                Path(str(path) + '.sha1').write_text(hashlib.sha1(path.read_bytes()).hexdigest())

            capture = root / 'maven-arguments'
            fake_maven = root / 'mvn'
            fake_maven.write_text(
                '#!/usr/bin/env bash\n'
                "printf '__CALL__\\0' >> \"${MAVEN_CAPTURE}\"\n"
                "printf '%s\\0' \"$@\" >> \"${MAVEN_CAPTURE}\"\n"
            )
            fake_maven.chmod(0o755)
            fake_gpg = root / 'gpg'
            fake_gpg.write_text('#!/usr/bin/env bash\nexit 0\n')
            fake_gpg.chmod(0o755)

            env = dict(
                os.environ,
                MVN=str(fake_maven),
                GPG=str(fake_gpg),
                MAVEN_CAPTURE=str(capture),
                REPOSITORY_DIRECTORY=str(repository),
                STAGING_PROFILE_ID='test-profile',
                NEXUS_URL='https://nexus.example.test/',
                SERVER_ID='test-server',
                NEXUS_STAGING_PLUGIN_VERSION='1.7.0',
                CUSTOM_OPTIONS='-DcustomOption=value',
            )
            result = subprocess.run(
                ['bash', str(SCRIPT)], env=env, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, universal_newlines=True)

            self.assertEqual(result.returncode, 0, result.stderr)
            arguments = capture.read_bytes().decode().split('\0')
            self.assertEqual(arguments.count('__CALL__'), 1)
            arguments.remove('__CALL__')
            self.assertEqual(arguments.count('-N'), 1)
            goal = 'org.sonatype.plugins:nexus-staging-maven-plugin:1.7.0:deploy-staged-repository'
            self.assertEqual(arguments.count(goal), 1)
            self.assertIn('-DnexusUrl=https://nexus.example.test/', arguments)
            self.assertIn('-DserverId=test-server', arguments)
            self.assertIn('-DstagingProfileId=test-profile', arguments)
            self.assertIn('-DcustomOption=value', arguments)
            self.assertIn('-DautoReleaseAfterClose=false', arguments)
            self.assertIn('-DkeepStagingRepositoryOnFailure=false', arguments)
            self.assertIn('-DkeepStagingRepositoryOnCloseRuleFailure=true', arguments)


if __name__ == '__main__':
    unittest.main()
