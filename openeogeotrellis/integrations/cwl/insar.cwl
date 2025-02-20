cwlVersion: v1.0
class: CommandLineTool
baseCommand: OpenEO_insar.py
requirements:
  DockerRequirement:
    dockerPull: registry.stag.warsaw.openeo.dataspace.copernicus.eu/rand/openeo_insar:1.2
inputs:
  input_base64_json:
    type: string
    inputBinding:
      position: 1
outputs:
  output_file:
    type:
      type: array
      items: File
    outputBinding:
      glob: "*2images*"
