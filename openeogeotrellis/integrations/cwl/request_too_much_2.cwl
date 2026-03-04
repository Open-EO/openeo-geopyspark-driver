#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool

requirements:
  DockerRequirement:
    dockerPull: vito-docker.artifactory.vgt.vito.be/openeo-geopyspark-driver-example-stac-catalog:1.8
  ResourceRequirement:
    ramMin: 999000 # 999Gb

baseCommand: [ "sh", "-c", "cp /data/* ." ]
inputs: [ ]
outputs:
  output:
    type: Directory
    outputBinding:
      glob: .
