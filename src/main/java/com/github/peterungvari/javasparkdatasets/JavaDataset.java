package com.github.peterungvari.javasparkdatasets;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

public class JavaDataset<T> {

    private Dataset<T> ds;

    public JavaDataset(Dataset<T> ds) {
        this.ds = ds;
    }

    public static <T> JavaDataset<T> of(SparkSession session, JavaRDD<T> data, Class<T> clazz) {
        return of(session.createDataset(data.rdd(), Encoders.bean(clazz)));
    }

    public static <T> JavaDataset<T> of(Dataset<T> ds) {
        return new JavaDataset<>(ds);
    }

    public Dataset<T> toDataset() {
        return ds;
    }

    public <U> JavaDataset<U> map(MapFunction<T, U> mapper, Class<U> clazz) {
        return of(ds.map(mapper, Encoders.bean(clazz)));
    }

    public <U> JavaDataset<U> map(MapFunction<T, U> mapper, Encoder<U> enc) {
        return of(ds.map(mapper, enc));
    }

    public T reduce(ReduceFunction<T> reducer) {
        return ds.reduce(reducer);
    }

    public <K> JavaKeyValueGroupedDataset<K, T> groupByKey(MapFunction<T, K> mapper, Class<K> clazz) {
        return JavaKeyValueGroupedDataset.of(ds.groupByKey(mapper, Encoders.bean(clazz)));
    }

    public <K> JavaKeyValueGroupedDataset<K, T> groupByKey(MapFunction<T, K> mapper, Encoder<K> enc) {
        return JavaKeyValueGroupedDataset.of(ds.groupByKey(mapper, enc));
    }

    public JavaDataset<T> filter(FilterFunction<T> filter) {
        return of(ds.filter(filter));
    }

    public Dataset<Row> toDF() {
        return ds.toDF();
    }

    public void printSchema() {
        ds.printSchema();
    }

    public void explain(boolean extended) {
        ds.explain(extended);
    }

    public void explain() {
        ds.explain();
    }

    public String[] columns() {
        return ds.columns();
    }

    public void show(int numRows) {
        ds.show(numRows);
    }

    public void show() {
        ds.show();
    }

    public void show(boolean truncate) {
        ds.show(truncate);
    }

    public void show(int numRows, boolean truncate) {
        ds.show(numRows, truncate);
    }

    public void show(int numRows, int truncate) {
        ds.show(numRows, truncate);
    }

    public JavaDataset<T> sortWithinPartitions(String sortCol, String... sortCols) {
        return of(ds.sortWithinPartitions(sortCol, sortCols));
    }

    public JavaDataset<T> sort(String sortCol, String... sortCols) {
        return of(ds.sort(sortCol, sortCols));
    }

    public JavaDataset<T> sort(Column... sortExprs) {
        return of(ds.sort(sortExprs));
    }

    public Column col(String colName) {
        return ds.col(colName);
    }

    public JavaDataset<T> as(String alias) {
        return of(ds.as(alias));
    }

    public T head() {
        return ds.head();
    }

    public T first() {
        return ds.first();
    }

    public List<T> takeAsList(int n) {
        return ds.takeAsList(n);
    }

    public List<T> collectAsList() {
        return ds.collectAsList();
    }

    public long count() {
        return ds.count();
    }

    public JavaDataset<T> repartition(int numPartitions) {
        return of(ds.repartition(numPartitions));
    }

    public JavaDataset<T> coalesce(int numPartitions) {
        return of(ds.coalesce(numPartitions));
    }

    public JavaDataset<T> distinct() {
        return of(ds.distinct());
    }

    public JavaDataset<T> persist() {
        return of(ds.persist());
    }

    public JavaDataset<T> cache() {
        return of(ds.cache());
    }

    public JavaDataset<T> persist(StorageLevel newLevel) {
        return of(ds.persist(newLevel));
    }

    public JavaDataset<T> unpersist(boolean blocking) {
        return of(ds.unpersist(blocking));
    }

    public JavaDataset<T> unpersist() {
        return of(ds.unpersist());
    }

    public JavaRDD<T> javaRDD() {
        return ds.javaRDD();
    }

    public void createOrReplaceTempView(String viewName) {
        ds.createOrReplaceTempView(viewName);
    }

    public <U> JavaDataset<Tuple2<T, U>> joinWith(JavaDataset<U> other, Column condition, String joinType) {
        return of(ds.joinWith(other.toDataset(), condition, joinType));
    }

    public <U> JavaDataset<Tuple2<T, U>> joinWith(JavaDataset<U> other, Column condition) {
        return of(ds.joinWith(other.toDataset(), condition));
    }
}
