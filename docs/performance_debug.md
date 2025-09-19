# Troubleshooting performance issues

Performance is a big concern for the backend and its users. We explain some key concepts and ways to troubleshoot potential issues,
allowing to identify the root cause.

## Step 1: categorize your issue

Given the broad API provided by openEO, there are multiple types of issues that are often labeled 'performance issue'.
It is a good idea to be more specific about the type of issue you are facing, especially when communicating to the developers.

These are some common cases that you might encounter:

1. Batch job latency: batch jobs are scheduled on processing resources, but take time to start, often due to many users running jobs at the same time.
2. Batch job slow wall time: the job takes a relatively long time to complete, but the credit cost is low.
3. Batch job high credit cost: the job costs more than expected
4. Slow result download
5. Slow API responses for common operations like job listings
6. Slow /execute requests (synchronous processing)

Of these cases, (3) and (6) are the ones that we are actually discussing further. Cases (1) and (2) can often be considered
'by design' and depend more on the capacity of underlying cloud resources, which is tracked by the backend provider.
The other cases may be signs of instability, but can also be caused by things like slow network connections.

The difference between case 2 and 3 is the concept of 'wall time' versus 'total cpu time': the wall time on the clock is
what many people use to judge performance, but a bad indicator, because a job could be doing nothing at all for a lot of the time.
The total cpu time is the best indicator of performance, because it counts time spent on all workers.

## Step 2: Identify root cause

### Cause 1: slow input data reading

OpenEO supports reading a huge variety of input data, and very often this is the culprit of slow processing. In some cases,
one inefficient 'load_collection'/'load_stac' can dominate the cost of the entire job.

To help with identifying slow reading, specific log messages have been added:

```
load_collection: data was loaded with an average speed of: 6.764 Megapixel per second.
Stage 18 produced 442.67 MB in 5 minutes - load_collection: read by input product.
```

Note that when it says it took '5 minutes', this is total cpu time spent by multiple workers, so the actual wall time was
a lot less.

### Cause 2: bad partitioning

The openEO process graph needs to be subdivided into concrete tasks that can be executed on a distributed processing cluster.
Spark is responsible for most of this, but it is controlled by the 'partitioner' that is configured on the datacube.

When performing distributed processing, performance does not increase linearly by subdividing the work into smaller tasks,
instead, it is governed by [Amdahl's law](https://en.wikipedia.org/wiki/Amdahl%27s_law), and performance increase reaches a limit
when the overhead of managing the tasks becomes too high.

In addition to that, in Spark, having a suboptimal partitioner can even result in unnecessary 'shuffles' of your data, which
are bad for performance especially for larger datasets.

Partitioner problems are mostly visible in the Spark UI, which is not available to end users.

There's two things to watch out for in the Spark UI:

- high task counts, with task duration statistics that are not normally distributed (like a few long running tasks and huge amount of smaller ones)
- high stage counts, meaning potentially too many shuffles

Note that you can often focus on the stages that have the longest total task time (this can be different from duration).
These tasks times are printed in the user logs, allowing to easily identify the stages that are causing the most trouble.

### Cause 3: driver performing too much work

Sometimes, the executors are not doing anything at all, and are simply allocated while the driver is doing work or waiting
for something to happen. This is not ideal, because total cpu hours might be adding up (unless if Spark removes executors
via dynamic allocation).

The Spark UI shows this, but also user logs show when the last stage was finished. If there's big gaps when all stages
have finished and it takes long before a new message is printed, then it makes sense to figure out what was happening there.
