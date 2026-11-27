#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool

requirements:
  - class: DockerRequirement
    dockerPull: ghcr.io/open-eo/example-stac-catalog:1.1.0
  - class: ResourceRequirement
    ramMin: 1
    ramMax: 1
  - class: ResourceRequirement
    ramMin: 999000 # 999Gb
    ramMax: 999000 # 999Gb
  - class: ResourceRequirement
    ramMin: 1
    ramMax: 1

baseCommand: [ "sh", "-c", "cp /data/* ." ]
inputs: [ ]
outputs:
  output:
    type: Directory
    outputBinding:
      glob: .
