# QUESTIONS

## What distributed computing principle(s) did your project rely on? Explain how your workload is split and parallelized across a cluster (e.g., partitioning strategy, shufe operations).
The project relies on data parallelism using Apache Spark. The dataset is split into multiple partitions that are processed independently across available CPU cores. Transformations such as filtering and aggregation are executed in parallel on these partitions. Operations like groupBy involve shuffle phases, where data is redistributed across partitions based on keys (e.g., department and year). Additionally, Parquet partitioning allows Spark to only read relevant data, improving parallel efficiency and reducing I/O.

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
Several performance optimizations were applied in this project to improve data processing efficiency. First, we defined a restricted schema, selecting only the useful columns from the original dataset (9 instead of more than 200), which significantly reduced the amount of data read and processed. We also converted the data into appropriate types (timestamp, double) to enable efficient analytical operations.</br>
We then introduced repartitioning to better distribute the workload across available cores and improve parallel execution. For storage, we used Parquet  partitioning, allowing Spark to perform partition pruning and read only the necessary data during queries. Additionally, the use of a columnar format reduced I/O by reading only the required columns.

## If your dataset were 100x larger, what would break in your current pipeline and how would you fx it? Think about bottlenecks, data skew, memory limits, and infrastructure changes.
If the dataset were 100 times larger, several bottlenecks would appear in the current pipeline. First, memory limitations would become critical, as the dataset might no longer fit in memory, leading to increased disk usage and slower processing. Shuffle operations, especially during aggregations like groupBy, would also become significantly more expensive and could dominate execution time. Additionally, data skew could create imbalanced partitions, where some nodes process much more data than others, reducing overall efficiency.</br>
To address these issues, the pipeline would need to scale horizontally by increasing the number of nodes and executors. We would also improve the partitioning strategy to better distribute the data and reduce skew. Techniques such as adaptive query execution, optimized file sizes, and selective caching could further enhance performance. Finally, using distributed storage systems such as HDFS would be necessary to handle the increased data volume efficiently.

## What was the most valuable thing you learned in this course, and how did it infuence your project?
The most valuable lesson from this course was understanding that data storage and organization are as important as computation in Big Data systems. Initially, we focused mainly on processing logic, but we quickly realized that performance is largely driven by how data is structured, stored, and accessed.
