package com.github.peterungvari.javasparkdatasets;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class JavaDatasetTest implements Serializable {

    private static final long serialVersionUID = 1L;

    private SparkSession session = SparkSession.builder().master("local[2]").getOrCreate();

    @Before
    public void setUp() {

    }

    @Test
    public void testMap1() {
        Dataset<String> ds = session.createDataset(Arrays.asList("foo", "bar", "baz"), Encoders.STRING());
        JavaDataset<String> mappedDs = JavaDataset.of(ds).map(String::toUpperCase, Encoders.STRING());
        assertEquals(Arrays.asList("FOO", "BAR", "BAZ"), mappedDs.collectAsList());
    }

    @Test
    public void testMap2() {
        Dataset<String> ds = session.createDataset(Arrays.asList("foo", "bar", "baz"), Encoders.STRING());
        JavaDataset<Person> mappedDs = JavaDataset.of(ds).map(Person::new, Person.class);
        assertEquals("foo", mappedDs.first().getName());
    }

    @Test
    public void testReduce() {
        Dataset<String> ds = session.createDataset(Arrays.asList("foo", "bar", "baz"), Encoders.STRING());
        String concatenated = JavaDataset.of(ds).reduce(String::concat);
        assertEquals(9, concatenated.length());
        assertTrue(concatenated.contains("foo"));
        assertTrue(concatenated.contains("bar"));
        assertTrue(concatenated.contains("baz"));
    }

    @Test
    public void testGroupByKey() {
        Dataset<String> ds = session.createDataset(Arrays.asList("foo", "bar", "baz"), Encoders.STRING());
        JavaDataset<String> countDs = JavaDataset.of(ds)
                .groupByKey(word -> String.valueOf(word.charAt(0)).toUpperCase(), Encoders.STRING())
                .mapGroups((initial, iterator) -> {
                    int s = 0;
                    while (iterator.hasNext()) {
                        iterator.next();
                        s++;
                    }
                    return initial + s;
                }, Encoders.STRING());

        List<String> counts = countDs.collectAsList();
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
        countDs.toDataset().cache();
        assertEquals(2, countDs.count());
        assertEquals(2, counts.size());
        assertTrue(counts.contains(new PersonItemCount(foo, 17)));
        assertTrue(counts.contains(new PersonItemCount(bar, 3)));
    }

}