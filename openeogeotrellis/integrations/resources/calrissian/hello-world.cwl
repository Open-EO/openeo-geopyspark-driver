cwlVersion: v1.2
class: CommandLineTool
baseCommand: echo
stdout: hello-stdout.txt

inputs:
  message:
    type: string
    default: "Hello World"
    inputBinding:
      position: 1

outputs:
  stdout:
    type: stdout
