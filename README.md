# Test Suite for Apache BookKeeper

Test suite for the Apache BookKeeper project, and analysis of their adequacy through coverage tools such as Ba-Dua for data coverage, Jaco for statement coverage and branch coverage. Further quality controls of the developed tests were carried out through the Pitest tool to verify their robustness to SUT mutations.

The following classes have been tested:
```
org.apache.bookkeeper.client.BookKeeper
```
```
org.apache.bookkeeper.bookie.storage.ldb.ReadCache
```



## Apache BookKeeper Overview

Apache BookKeeper is a scalable, fault tolerant and low latency storage service optimized for append-only workloads.

It is suitable for being used in following scenarios:

- WAL (Write-Ahead-Logging), e.g. HDFS NameNode, Pravega.
- Message Store, e.g. Apache Pulsar.
- Offset/Cursor Store, e.g. Apache Pulsar.
- Object/Blob Store, e.g. storing state machine snapshots.

## Some results of adequacy


|  Class | Statement Coverage | Branch Coverage | Mutation Coverage |
|:----------:|:----------------------:|:-------------------:|:---------------------:|
| BookKeeper |           49%          |         36%         |          47%          |
| ReadCache  |          100%          |         70%         |          68%          |


## Documentation

Testing [report](https://github.com/callbrok/TestSuite_BookKeeper/blob/50d0b85558eadfc7a2e9d5e6a2e471ac0773b43e/report/testing_report.pdf) ( :it: ) made with all the results, improvements and considerations.



