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
