package com.github.peterungvari.javasparkdatasets;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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
}