#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool

requirements:
  - class: DockerRequirement
    dockerPull: openeo-docker-local-prod.repo.vito.be/openeo-geopyspark-driver-example-stac-catalog:1.10
  - class: ResourceRequirement
    ramMin: 999000 # 999Gb
    ramMax: 999000 # 999Gb

baseCommand: [ "sh", "-c", "cp /data/* ." ]
inputs: [ ]
outputs:
  output:
    type: Directory
    outputBinding:
      glob: .
