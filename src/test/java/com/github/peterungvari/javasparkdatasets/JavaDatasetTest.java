package com.github.peterungvari.javasparkdatasets;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.CacheManager;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.storage.StorageLevel;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class JavaDatasetTest implements Serializable {

    private static final long serialVersionUID = 1L;

    private SparkSession session = SparkSession.builder().master("local[2]").getOrCreate();

    @Before
    public void setUp() {

    }

    private JavaDataset<String> createStringDs() {
        return createStringDs("foo", "bar", "baz");
    }

    private JavaDataset<String> createStringDs(String... strings) {
        Dataset<String> ds = session.createDataset(Arrays.asList(strings), Encoders.STRING());
        return JavaDataset.of(ds);
    }

    @Test
    public void testMap1() {
        JavaDataset<String> mappedDs = createStringDs().map(String::toUpperCase, Encoders.STRING());
        assertEquals(Arrays.asList("FOO", "BAR", "BAZ"), mappedDs.collectAsList());
    }

    @Test
    public void testMap2() {
        JavaDataset<Person> mappedDs = createStringDs().map(Person::new, Person.class);
        assertEquals("foo", mappedDs.first().getName());
    }

    @Test
    public void testReduce() {
        String concatenated = createStringDs().reduce(String::concat);
        assertEquals(9, concatenated.length());
        assertTrue(concatenated.contains("foo"));
        assertTrue(concatenated.contains("bar"));
        assertTrue(concatenated.contains("baz"));
    }

    @Test
    public void testGroupByKey() {
        JavaKeyValueGroupedDataset<String, String> jkvgds = createStringDs()
                .groupByKey(word -> String.valueOf(word.charAt(0)).toUpperCase(), Encoders.STRING());

        assertEquals(jkvgds.keys().collectAsList(), jkvgds.toKeyValueGroupedDataset().keys().collectAsList());

        JavaDataset<String> countDs = jkvgds.mapGroups((initial, iterator) -> {
                    int s = 0;
                    while (iterator.hasNext()) {
                        iterator.next();
                        s++;
                    }
                    return initial + s;
                }, Encoders.STRING());

        List<String> counts = countDs.collectAsList();
        assertEquals(countDs.toDF().collectAsList(), countDs.toDataset().toDF().collectAsList());
        assertEquals(2, counts.size());
        assertTrue(counts.contains("F1"));
        assertTrue(counts.contains("B2"));
    }

    @Test
    public void testGroupByKey2() {
        Person foo = new Person("foo");
        Person bar = new Person("bar");
        Dataset<PersonItemCount> ds = session.createDataset(
                Arrays.asList(new PersonItemCount(foo, 12), new PersonItemCount(bar, 3), new PersonItemCount(foo, 5)),
                Encoders.bean(PersonItemCount.class));

        JavaDataset<PersonItemCount> countDs = JavaDataset.of(ds)
                .groupByKey(PersonItemCount::getPerson, Person.class)
                .mapGroups((person, iterator) -> {
                    int s = 0;
                    while (iterator.hasNext()) {
                        s += iterator.next().getItemCount();
                    }
                    return new PersonItemCount(person, s);
                }, PersonItemCount.class);

        List<PersonItemCount> counts = countDs.takeAsList(3);
        assertEquals(2, counts.size());
        assertTrue(counts.contains(new PersonItemCount(foo, 17)));
        assertTrue(counts.contains(new PersonItemCount(bar, 3)));
    }

    @Test
    public void testFilter() {
        List<String> strings = createStringDs().filter(s -> s.contains("b")).collectAsList();
        assertEquals(2, strings.size());
        assertTrue(strings.contains("bar"));
        assertTrue(strings.contains("baz"));
    }

    @Test
    public void testSortWithinPartitions() {
        List<String> strings = createStringDs().coalesce(1).sortWithinPartitions("value").collectAsList();
        assertEquals(Arrays.asList("bar", "baz", "foo"), strings);
    }

    @Test
    public void testSort1() {
        List<String> strings = createStringDs().sort("value").collectAsList();
        assertEquals(Arrays.asList("bar", "baz", "foo"), strings);
    }

    @Test
    public void testSort2() {
        JavaDataset<String> ds = createStringDs();
        List<String> strings = ds.sort(ds.col("value")).collectAsList();
        assertEquals(Arrays.asList("bar", "baz", "foo"), strings);
    }

    @Test
    public void testHead() {
        assertEquals("foo", createStringDs().head());
    }

    @Test
    public void testFirst() {
        assertEquals("foo", createStringDs().first());
    }

    @Test
    public void testTakeAsList() {
        JavaDataset<String> stringDs = createStringDs();
        assertEquals(stringDs.toDataset().takeAsList(4), stringDs.toDataset().takeAsList(4));
    }

    @Test
    public void testCollectAsList() {
        JavaDataset<String> stringDs = createStringDs();
        assertEquals(stringDs.toDataset().collectAsList(), stringDs.toDataset().collectAsList());
    }

    @Test
    public void testCount() {
        JavaDataset<String> stringDs = createStringDs();
        assertEquals(stringDs.toDataset().count(), stringDs.count());
    }

    @Test
    public void testRepartition() {
        JavaDataset<String> ds3 = createStringDs().repartition(3);
        assertEquals(3, ds3.javaRDD().getNumPartitions());
        assertEquals(2, ds3.repartition(2).javaRDD().getNumPartitions());
    }

    @Test
    public void testDistinct() {
        JavaDataset<String> ds = createStringDs("foo", "bar", "baz", "bar", "foo").distinct();
        assertEquals(3, ds.count());
        List<String> strings = ds.collectAsList();
        assertTrue(strings.contains("foo"));
        assertTrue(strings.contains("bar"));
        assertTrue(strings.contains("baz"));
    }

    @Test
    public void testPersist1() {
        JavaDataset<String> ds = createStringDs();
        ds.persist();
        ds.head(); //caching is lazy
        CacheManager cacheManager = session.sharedState().cacheManager();
        assertTrue(cacheManager.lookupCachedData(ds.toDataset().logicalPlan()).isDefined());
        ds.unpersist(true);
        assertFalse(cacheManager.lookupCachedData(ds.toDataset().logicalPlan()).isDefined());

    }

    @Test
    public void testPersist2() {
        JavaDataset<String> ds = createStringDs();
        ds.persist(StorageLevel.DISK_ONLY());
        ds.head(); //caching is lazy
        CacheManager cacheManager = session.sharedState().cacheManager();
        assertTrue(cacheManager.lookupCachedData(ds.toDataset().logicalPlan()).isDefined());
        ds.unpersist(false);
        assertFalse(cacheManager.lookupCachedData(ds.toDataset().logicalPlan()).isDefined());

    }

    @Test
    public void testCache() {
        JavaDataset<String> ds = createStringDs();
        ds.cache();
        ds.head(); //caching is lazy
        CacheManager cacheManager = session.sharedState().cacheManager();
        assertTrue(cacheManager.lookupCachedData(ds.toDataset().logicalPlan()).isDefined());
        ds.unpersist();
        assertFalse(cacheManager.lookupCachedData(ds.toDataset().logicalPlan()).isDefined());
    }

    @Test
    public void testJoinWith1() {
        JavaDataset<String> ds = createStringDs("apple", "elephant", "tiger");
        List<Tuple2<String, String>> pairs = ds.as("ds1").joinWith(ds.as("ds2"),
                expr("substring(ds1.value, -1, 1)").equalTo(expr("substring(ds2.value, 1, 1)"))).collectAsList();
        assertEquals(2, pairs.size());
        assertTrue(pairs.contains(new Tuple2<>("apple", "elephant")));
        assertTrue(pairs.contains(new Tuple2<>("elephant", "tiger")));
    }

    @Test
    public void testJoinWith2() {
        JavaDataset<String> ds = createStringDs("apple", "elephant", "tiger");
        List<Tuple2<String, String>> pairs = ds.as("ds1").joinWith(ds.as("ds2"),
                expr("substring(ds1.value, -1, 1)").equalTo(expr("substring(ds2.value, 1, 1)")), "left").collectAsList();
        assertEquals(3, pairs.size());
        assertTrue(pairs.contains(new Tuple2<>("apple", "elephant")));
        assertTrue(pairs.contains(new Tuple2<>("elephant", "tiger")));
        assertTrue(pairs.contains(new Tuple2<>("tiger", (String) null)));
    }

    @Test
    public void testColumns1() {
        String[] columns = createStringDs().columns();
        assertEquals(1, columns.length);
        assertEquals("value", columns[0]);
    }

    @Test
    public void testColumns2() {
        JavaDataset<Person> ds = JavaDataset.of(session.createDataset(Collections.singletonList(new Person("Jack")),
                Encoders.bean(Person.class)));

        String[] columns = ds.columns();
        assertEquals(1, columns.length);
        assertEquals("name", columns[0]);
    }

    @Test
    public void testCreateOrReplaceTempView() {
        createStringDs().createOrReplaceTempView("strings");
        List<Row> rows = session.sql("select * from strings").collectAsList();
        assertEquals(3, rows.size());
        for (Row row : rows) {
            String s = row.getAs("value");
            assertTrue("foobarbaz".contains(s));
        }
    }

    @Test
    @Ignore
    public void testAtYourOwnRisk() {
        JavaDataset<String> ds = createStringDs();
        ds.explain();
        ds.explain(true);
        ds.show();
        ds.show(1);
        ds.show(true);
        ds.show(2, false);
        ds.show(2, 2);
        ds.printSchema();
    }

    @Test
    public void testOfJavaRDD() {
        JavaRDD<Person> personRDD = JavaSparkContext.fromSparkContext(
                session.sparkContext()).parallelize(Collections.singletonList(new Person("Joe")));

        JavaDataset<Person> ds = JavaDataset.of(session, personRDD, Person.class);
        assertEquals("Joe", ds.head().getName());
    }
}