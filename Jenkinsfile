#!/usr/bin/env groovy

/*
    This Jenkinsfile is used to provide snapshot builds using the VITO CI system.
    This Jenkinsfile uses the internal "biggeo/jenkinslib.git" library.
    Information about the pythonPipeline method can be found in pythonPipeline.groovy
*/

@Library('lib')_

pythonPipeline {
  package_name = 'openeo-geopyspark'
  wipeout_workspace = true
  hadoop = true
  pre_test_script = 'pre_test.sh'
  extra_env_variables = [
    /* Set pytest `basetemp` inside Jenkins workspace. (Note: this is intentionally Jenkins specific, instead of a global pytest.ini thing.) */
    "PYTEST_DEBUG_TEMPROOT=pytest-tmp",
  ]
  python_version = ["3.8"]
  docker_registry = 'vito-docker-private.artifactory.vgt.vito.be'
  downstream_job = 'openEO/openeo-integrationtests'
  wheel_repo = 'python-openeo'
  wheel_repo_dev = 'python-openeo'
  test_module_name = 'openeogeotrellis'
  extras_require = 'dev'
  upload_dev_wheels = false
  pep440 = true
  venv_rpm_deps = ['gcc-c++', 'kstart', 'krb5-devel']
  custom_test_image = 'vito-docker.artifactory.vgt.vito.be/almalinux8.5-spark-py-openeo:3.4.0'
  extra_container_volumes = [
    '/data/MTDA:/data/MTDA:ro'
  ]
}
