# DGGS support in openEO

# Why HealPix DGGS

A discrete global grid system defines a global grid.

The Healpix nested layout follows a quad-tree structure, which allows for efficient spatial indexing and querying. 
Cells in healpix are equal-area, allowing correct statistical operations without requiring complex math.

A  DGGS defines a single fixed grid, so it inherently has fewer variables compared to mixing cubes with different
projection systems, resolutions and pixel offsets. This makes it less error prone and even easier to use.

The benefits of DGGS are especially relevant for medium resolution data like Sentinel-3, 5p, PROBA-V and ERA5.

# DGGS in openEO Geotrellis

An experimental implementation was used to validate the possibility of representing data cubes in a HealPix DGGS.

The main insight is that we can represent a DGGS in tabular structures, so Spark SQL is a natural fit for this.
A conversion from DGGS to raster data cube was also implemented.

The experiment also introduced other new design elements:

- A process registry in Sala, to allow automatic discovery of new processes on the Python side.
- Use of new capability in UCar netcdf to read directly from S3.
- 2 datacube representations in Spark SQL: packed and scalar.

# Major work items

- New DGGS process definitions
- load_stac converts s3 hrefs
- A better design for writers
- Improve interface for readers
- Consider adding geoparquet writer

# Towards first use case

A simple first case would be to support the community that works with healpix, by allowing CDSE
to generate compatible data.