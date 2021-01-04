#!/usr/bin/env groovy

/*
    This Jenkinsfile is used to provide snapshot builds using the VITO CI system. Travis is used to provide publicly accessible test results.
    This Jenkinsfile uses the Jenkins shared library. (ssh://git@git.vito.local:7999/biggeo/jenkinslib.git)
    Information about the pythonPipeline method can be found in pythonPipeline.groovy
*/

@Library('lib')_

pythonPipeline {
  package_name = 'openeo-geopyspark'
  wipeout_workspace = true
  hadoop = false
  pre_test_script = 'pre_test.sh'
  extra_env_variables = ['TRAVIS=1']
  python_version = ["3.6","3.8"]
  docker_registry = 'vito-docker-private-dev.artifactory.vgt.vito.be'
  downstream_job = 'geo.OpenEO/openeo-geopyspark-integrationtests'
  wheel_repo = 'python-openeo'
  wheel_repo_dev = 'python-openeo'
  test_module_name = 'openeogeotrellis'
  extras_require = 'dev'
  upload_dev_wheels = false
}
