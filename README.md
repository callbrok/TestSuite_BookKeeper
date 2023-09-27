# Test Suite for Apache BookKeeper

Test suite (unit test and integration test) for the Apache BookKeeper project, and analysis of their adequacy through coverage tools such as [Ba-Dua](https://github.com/saeg/ba-dua) for data coverage,  [JaCoCo](https://github.com/jacoco/jacoco) for statement coverage and branch coverage. Further quality controls of the developed tests were carried out through [Pitest](https://github.com/hcoles/pitest) (aka PIT) tool, to verify their robustness to SUT mutations.

[Mockito](https://github.com/mockito/mockito) was used to simulate the execution environment and [GitHub Actions](https://github.com/features/actions) was used as a continuous integration tool.

The following classes have been tested:
```
org.apache.bookkeeper.client.BookKeeper

org.apache.bookkeeper.bookie.storage.ldb.ReadCache
```

Each tool is associated with a dedicated Maven profile, can be executed through the commands:

```
mvn clean verify -P jacoco
```
```
mvn clean verify -P badua
```
```
mvn clean test -P pit
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

[Testing report](https://github.com/callbrok/TestSuite_BookKeeper/blob/50d0b85558eadfc7a2e9d5e6a2e471ac0773b43e/report/testing_report.pdf) (:it:) made with all the results, improvements and considerations.



