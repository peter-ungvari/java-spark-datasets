package com.github.peterungvari.javasparkdatasets;

import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;

public class JavaKeyValueGroupedDataset<K, T> {

    private KeyValueGroupedDataset<K, T> kvgds;

    public JavaKeyValueGroupedDataset(KeyValueGroupedDataset<K, T> kvgds) {
        this.kvgds = kvgds;
    }

    public static <K, T> JavaKeyValueGroupedDataset<K, T> of(KeyValueGroupedDataset<K, T> kvgds) {
        return new JavaKeyValueGroupedDataset<>(kvgds);
    }

    public KeyValueGroupedDataset<K, T> toKeyValueGroupedDataset() {
        return kvgds;
    }

    public <U> JavaDataset<U> mapGroups(MapGroupsFunction<K, T, U> groupsMapper, Class<U> clazz) {
        return JavaDataset.of(kvgds.mapGroups(groupsMapper, Encoders.bean(clazz)));
    }

    public <U> JavaDataset<U> mapGroups(MapGroupsFunction<K, T, U> groupsMapper, Encoder<U> enc) {
        return JavaDataset.of(kvgds.mapGroups(groupsMapper, enc));
    }

    public JavaDataset<K> keys() {
        return JavaDataset.of(kvgds.keys());
    }

}
