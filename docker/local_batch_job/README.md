# Run openEO from Docker file

This method does not have access to the collection on Terrascope or dataspace.copernicus.eu.
External public STAC collections are usable.

Run graph with `local_batch_job.sh path/to/process_graph.json`
The output files will be written to the same folder as process_graph.json.

If the docker file has an error connecting to the internet, consider disabling VPN.
