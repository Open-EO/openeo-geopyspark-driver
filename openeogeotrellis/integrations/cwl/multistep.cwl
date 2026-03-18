#!/usr/bin/env cwl-runner
cwlVersion: v1.2
$graph:
  - id: test
    class: CommandLineTool
    baseCommand: /src/XXX.py
    requirements:
      - class: InitialWorkDirRequirement
        listing:
          - entryname: "arguments.json"
            entry: $(inputs)
      - class: DockerRequirement
        dockerPull: ghcr.io/test:test
      - class: NetworkAccess
        networkAccess: true
      - class: ResourceRequirement
        ramMin: 7000
        ramMax: 7000
        coresMin: 2
        coresMax: 7

    inputs:
      test:
        type: int

    arguments:
      - arguments.json

    outputs:
      test:
        type: int

  - id: scatter_node
    class: Workflow
    inputs:
      test:
        type: int

    requirements:
      - class: ScatterFeatureRequirement

    steps:
      scatter_node_step:
        scatter: [ InSAR_pairs ]
        scatterMethod: flat_crossproduct
        in:
      test:
        type: int

        out: [ test_out ]
        run: "#test"

    outputs:
      - id: scatter_node_out
        outputSource: scatter_node_step/test_out
        type: Directory[]

  - id: test
    class: CommandLineTool
    requirements:
      - class: DockerRequirement
        dockerPull: ghcr.io/test:test
      - class: NetworkAccess
        networkAccess: true

    baseCommand: ["/data/test.py", "collection.json"]
    inputs:
      test:
        type: int
    outputs:
      test:
        type: Directory
        outputBinding:
          glob: .

  - id: main
    class: Workflow
    inputs:
      test:
        type: int

    requirements:
      - class: SubworkflowFeatureRequirement

    steps:
      gatherer_node_step1:
        in:
          test:
            type: int
        out: [ scatter_node_out ]
        run: "#scatter_node"
      test:
        in:
          test:
            type: int
        out: [ test ]
        run: "#test"
    outputs:
      - id: gatherer_node_out
        outputSource: test/test
        type: Directory
