#!/usr/bin/env groovy

/*
    This Jenkinsfile is used to provide snapshot builds using the VITO CI system. Travis is used to provide publicly accessible test results.
    This Jenkinsfile uses the Jenkins shared library. (ssh://git@git.vito.local:7999/biggeo/jenkinslib.git)
    Information about the pythonPipeline method can be found in pythonPipeline.groovy
*/

@Library('lib')_

pythonPipeline {
  package_name = 'openeo_geopyspark'
  wipeout_workspace = true
  hadoop = true
  pre_test_script = 'pre_test.sh'
  extra_env_variables = 'TRAVIS=1'
  downstream_job = 'openeo-geopyspark-integrationtests'
}
