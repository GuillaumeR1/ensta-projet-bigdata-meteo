# QUESTIONS

## What distributed computing principle(s) did your project rely on? Explain how your workload is split and parallelized across a cluster (e.g., partitioning strategy, shufe operations).

## How did you choose your storage format and why? Discuss trade-offs between formats you considered (e.g., CSV vs Parquet vs Avro) in terms of schema evolution, compression, and read performance.
We compare the storage data using csv, avro and parquet. </br>
CSV
- expensive parsing because there isn't schema
- no compression then expensive storage is mandatory
- poor read performance

AVRO
- using a schema
- good for data exchange and streaming
- moderate compression
- limited for analytics because need to read all row

PARQUET
- high compression
- read only required columns
- reduces data scan
- best read performances

## What performance optimization(s) did you apply or would you apply with more time? Examples: partitioning, caching, broadcast joins, predicate pushdown, choice of API (RDD vs DataFrame), cluster sizing.

## If your dataset were 100x larger, what would break in your current pipeline and how would you fx it? Think about bottlenecks, data skew, memory limits, and infrastructure changes.
If the dataset were 100 times larger, several bottlenecks would appear in the current pipeline. First, memory limitations would become critical, as the dataset might no longer fit in memory, leading to increased disk usage and slower processing. Shuffle operations, especially during aggregations like groupBy, would also become significantly more expensive and could dominate execution time. Additionally, data skew could create imbalanced partitions, where some nodes process much more data than others, reducing overall efficiency.</br>
To address these issues, the pipeline would need to scale horizontally by increasing the number of nodes and executors. We would also improve the partitioning strategy to better distribute the data and reduce skew. Techniques such as adaptive query execution, optimized file sizes, and selective caching could further enhance performance. Finally, using distributed storage systems such as HDFS would be necessary to handle the increased data volume efficiently.

## What was the most valuable thing you learned in this course, and how did it infuence your project?
