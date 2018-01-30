package com.github.peterungvari.javasparkdatasets;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;

import java.io.Serializable;

public enum JavaSparkDataSets implements Serializable {
    INSTANCE;

    public static JavaSparkDataSets getInstance() {
        return INSTANCE;
    }

    public <T, U> Dataset<U> map(Dataset<T> ds, MapFunction<T, U> mapper, Class<U> clazz) {
        return ds.map(mapper, Encoders.bean(clazz));
    }

    public <T, U> Dataset<U> map(Dataset<T> ds, MapFunction<T, U> mapper, Encoder<U> enc) {
        return ds.map(mapper, enc);
    }

    public <T> T reduce(Dataset<T> ds, ReduceFunction<T> reducer) {
        return ds.reduce(reducer);
    }

    public <T, K> KeyValueGroupedDataset<K, T> groupByKey(Dataset<T> ds, MapFunction<T, K> mapper, Class<K> clazz) {
        return ds.groupByKey(mapper, Encoders.bean(clazz));
    }

    public <T, K> KeyValueGroupedDataset<K, T> groupByKey(Dataset<T> ds, MapFunction<T, K> mapper, Encoder<K> enc) {
        return ds.groupByKey(mapper, enc);
    }

    public <T, U, K> Dataset<U> mapGroups(
            KeyValueGroupedDataset<K, T> kvgds, MapGroupsFunction<K, T, U> groupsMapper, Class<U> clazz) {
        return kvgds.mapGroups(groupsMapper, Encoders.bean(clazz));
    }

    public <T, U, K> Dataset<U> mapGroups(
            KeyValueGroupedDataset<K, T> kvgds, MapGroupsFunction<K, T, U> groupsMapper, Encoder<U> enc) {
        return kvgds.mapGroups(groupsMapper, enc);
    }

    public <T> Dataset<T> filter(Dataset<T> ds, FilterFunction<T> filter) {
        return ds.filter(filter);
    }

}
