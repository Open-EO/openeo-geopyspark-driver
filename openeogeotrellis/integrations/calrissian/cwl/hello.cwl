cwlVersion: v1.0
class: CommandLineTool
baseCommand: echo
requirements:
  - class: DockerRequirement
    dockerPull: debian:stretch-slim
inputs:
  message:
    type: string
    default: "Hello World"
    inputBinding:
      position: 1
outputs:
  output_file:
    type: File
    outputBinding:
      glob: output.txt
stdout: output.txt
