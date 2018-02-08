# java-spark-datasets

*Spark Dataset wrapper for Java*

[![Build Status](https://travis-ci.org/peter-ungvari/java-spark-datasets.svg?branch=master)](https://travis-ci.org/peter-ungvari/java-spark-datasets)
[![Coverage Status](https://coveralls.io/repos/github/peter-ungvari/java-spark-datasets/badge.svg?branch=master)](https://coveralls.io/github/peter-ungvari/java-spark-datasets?branch=master)

### Features
- creating `JavaDataset<T>` from `JavaRDD<T>` or `DataSet<T>`
- support for lambdas in transformations (Java 8+)
- `KeyValueGroupedDataset` support
- using `Class<?>` parameters instead of `Encoders.bean(clazz)`
- local maven repository deployment :)


