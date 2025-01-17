# Geotrellis openEO processing

This page describes some implementation details about the openEO processing based on Geotrellis.

## General processing strategy

By design, the Geotrellis implementation relies on distributed processing via the [Apache Spark](https://spark.apache.org)
library. Spark gives us a framework to describe and execute distributed processing workflows that transform arbitrary collections
of objects. In Spark, these are called resilient distributed datasets, or RDDs.

Geotrellis is a framework that helps with representing Georeferenced raster and vector data and also supports Spark.
Hence, the objects in our Spark collections (RDD), are key-value pairs where the key contains a timestamp and
a row and column in a spatial grid. The value is multiband raster tile. Such a tile contains chunks of a
fixed size, by default 256x256 pixels, for each band in the openEO data cube.

So let's take the example of an 'apply' process in openEO, with the process set to 'abs'. In
this case, Spark can simply apply the absolute value process to each 2D tile in parallel. If a chain of processes is applied,
Spark and Geotrellis functionality is used to make sure that this all happens as efficiently as possible.

An important feature of Spark is that intermediate results are kept in memory whenever possible and only spilled to disk
when needed. Writing to disk is entirely hidden for the actual processing workflow. This is quite different from more traditional
batch processing workflows that commonly write a lot of intermediate results to disk.

Other aspects covered by the combination of Spark and Geotrellis are complex operations such as merging data cubes, resampling
or applying functions over the complete timeseries for a given pixel. In these cases, it is often needed to reorganize the
the dataset in various ways. The Geotrellis openEO implementation tries to optimize all these cases.

For UDF's, where user-provided Python code is executed, we similarly reorganize the dataset, depending on what
spatiotemporal subset of the datacube can be processed in parallel. Then the data is converted into a Python XArray object
which is passed on to the user code for transformation.


## Data Cubes

The main openEO concept implemented by this library is the `Raster data cube`, which is a multi-dimensional array of raster data.

This library does not support arbitrary multidimensional cubes, but rather focuses on the type of cubes that are most 
commonly encountered in earth observation. The most complex type that we can represent is the 4-dimensional space-time-bands cube.

This cube is mapped to a Geotrellis [`MultibandTileLayerRDD`](https://geotrellis.github.io/scaladocs/latest/geotrellis/spark/index.html#MultibandTileLayerRDD[K]=org.apache.spark.rdd.RDD[(K,geotrellis.raster.MultibandTile)]withgeotrellis.layer.Metadata[geotrellis.layer.TileLayerMetadata[K]])

For cubes with a temporal dimension, a `SpacetimeKey` is used, while spatial cubes use the `SpatialKey`.

Geotrellis documents this class and its associated metadata [here](https://geotrellis.readthedocs.io/en/latest/guide/core-concepts.html#layouts-and-tile-layers).

To be able to understand how these cubes work and behave, or how to correctly manipulate them, knowledge of the [Spark RDD concept](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
is essential. These are distributed datasets that (in production) live on multiple machines in a cluster, allowing openEO 
to effectively scale out computations. 

### Data cube loading: `load_collection` & `load_stac` 

Loading large EO datasets into a Spark RDD is complex, mainly due to the variety in data formats, but also because
IO is often a performance & stability bottleneck. Depending on the characteristics of the underlying storage system, the 
optimal loading strategy may also differ, which is why we support multiple code paths.

The main class for loading data from POSIX or Object Storage type of systems is the `FileLayerProvider`.

This `load_collection` implementation supports a number of features:

- Resampling at load time, improving both performance and correctness.
- Loading of GDAL supported formats
- Loading of Geotiff using a native implementation
- Applying masks at load time, reducing the amount of data to read
- Applying polygonal filtering at load time


### Data cube organization: partitioners

As mentioned, Spark will distribute data cubes over a cluster, into groups of data called `partitions`. A partition needs
to fit into memory for processing, so the size matters, and Spark is not able to know the optimal partitioning for any given problem.

Depending on procesess that are used, the optimal partitioning scheme can also change: for time series analysis, it would 
be optimal to group data for the same location with multiple observations in the same group. For other processes, like `resample_spatial`,
it may be needed to have information from neighbouring tiles, so a grouping per observation would be more optimal.
As a rule of thumb it is up to the process to check the partitioner, and change it if needed.

Whenever a partitioner is changed, the data will be shuffled, which is a costly operation. This is why the code often tries 
to cleverly avoid this where possible.

Assigning keys to partitions happens based on an indexing scheme. Usually, it is recommended to consider the spatiotemporal
nature of the data to select the optimal scheme. Geotrellis explains a few potential schemes [here](https://geotrellis.readthedocs.io/en/latest/guide/core-concepts.html#key-indexes).

#### Sparse partitioners

Data cubes can be sparse because openEO supports operations on polygons, that are not necessarily spatially adjacent. Examples
include the use of `aggregate_spatial` on a vector cube or the `filter_spatial` process on a vector cube.

When this case occurs, regular Geotrellis spatial partitioners tend to create too many partitions, because they consider the
full bounding box instead of the more detailed geometry locations. The same can occur for data which is irregular in the temporal
dimension.

The effect of having too many partitions, is huge numbers of Spark tasks that do not do anything, but still consume resources as
they are nevertheless scheduled. This becomes especially noticeable when the actual work that needs to be done is small.

Sparse partitioners avoid this problem by determining all of the SpacetimeKeys up front. We also store the list of keys in
the partitioner itself, allowing certain operations to be implemented more efficiently.

## openEO processes

This library implements a large part of the openEO process specification, mainly by using Geotrellis functionality.

Depending on the type of process, the implementations can be found in 2 main locations: OpenEOProcesses and OpenEOProcessScriptBuilder.

`OpenEOProcesses` implements the Scala part of predefined processes that operate on full data cubes. This Scala code is usually
invoked by a Python counterpart in [`GeopysparkDataCube`](https://github.com/Open-EO/openeo-geopyspark-driver/blob/master/openeogeotrellis/geopysparkdatacube.py).
Some of this Scala code may be used by multiple openEO process implementations. For instance, openEO `reduce_dimension` and `apply_dimension` can in some cases
use the same code.

`OpenEOProcessScriptBuilder` supports openEO processes that operate on arrays of numbers, which are often encountered in
openEO `child processes`, as explained [here](https://api.openeo.org/#section/Processes/Process-Graphs). This part of the 
work is not distributed on Spark, so operates on chunks of data that fit in memory. It does use Geotrellis which generally has
quite well performing implementations for most basic processes.



