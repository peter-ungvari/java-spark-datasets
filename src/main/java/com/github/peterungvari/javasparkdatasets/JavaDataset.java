package com.github.peterungvari.javasparkdatasets;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import scala.annotation.varargs;

import java.util.Iterator;
import java.util.List;

public class JavaDataset<T> {

    private Dataset<T> ds;

    public JavaDataset(Dataset<T> ds) {
        this.ds = ds;
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

    public T reduce(Dataset<T> ds, ReduceFunction<T> reducer) {
        return ds.reduce(reducer);
    }

    public <K> KeyValueGroupedDataset<K, T> groupByKey(MapFunction<T, K> mapper, Class<K> clazz) {
        return ds.groupByKey(mapper, Encoders.bean(clazz));
    }

    public <K> JavaKeyValueGroupedDataset<K, T> groupByKey(MapFunction<T, K> mapper, Encoder<K> enc) {
        return JavaKeyValueGroupedDataset.of(ds.groupByKey(mapper, enc));
    }

    public JavaDataset<T> filter(Dataset<T> ds, FilterFunction<T> filter) {
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

    public boolean isLocal() {
        return ds.isLocal();
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

    public Dataset<T> sortWithinPartitions(String sortCol, String... sortCols) {
        return ds.sortWithinPartitions(sortCol, sortCols);
    }

    public Dataset<T> sort(String sortCol, String... sortCols) {
        return ds.sort(sortCol, sortCols);
    }

    public Dataset<T> sort(Column... sortExprs) {
        return ds.sort(sortExprs);
    }

    public Column col(String colName) {
        return ds.col(colName);
    }

    public Dataset<T> as(String alias) {
        return ds.as(alias);
    }

    public T[] head(int n) {
        return (T[]) ds.head(n);
    }

    public T head() {
        return ds.head();
    }

    public T first() {
        return ds.first();
    }

    public T[] take(int n) {
        return (T[]) ds.take(n);
    }

    public List<T> takeAsList(int n) {
        return ds.takeAsList(n);
    }

    public T[] collect() {
        return (T[]) ds.collect();
    }

    public List<T> collectAsList() {
        return ds.collectAsList();
    }

    public Iterator<T> toLocalIterator() {
        return ds.toLocalIterator();
    }

    public long count() {
        return ds.count();
    }

    public JavaDataset<T> repartition(int numPartitions) {
        return of(ds.repartition(numPartitions));
    }

    public JavaDataset<T> repartition(int numPartitions, Column... partitionExprs) {
        return of(ds.repartition(numPartitions, partitionExprs));
    }

    public JavaDataset<T> repartition(Column... partitionExprs) {
        return of(ds.repartition(partitionExprs));
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

    public void createTempView(String viewName) throws AnalysisException {
        ds.createTempView(viewName);
    }

    public void createOrReplaceTempView(String viewName) {
        ds.createOrReplaceTempView(viewName);
    }
}
