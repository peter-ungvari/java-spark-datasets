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

public class JavaSparkDataSetsTest implements Serializable {

    private JavaSparkDataSets jsds = JavaSparkDataSets.getInstance();
    private SparkSession session = SparkSession.builder().master("local[2]").getOrCreate();

    @Before
    public void setUp() {

    }

    @Test
    public void testMap1() {
        Dataset<String> ds = session.createDataset(Arrays.asList("foo", "bar", "baz"), Encoders.STRING());
        Dataset<String> mappedDs = jsds.map(ds, String::toUpperCase, Encoders.STRING());
        assertArrayEquals(new String[] {"FOO", "BAR", "BAZ"}, (String[])mappedDs.collect());
    }

    @Test
    public void testMap2() {
        Dataset<String> ds = session.createDataset(Arrays.asList("foo", "bar", "baz"), Encoders.STRING());
        Dataset<Person> mappedDs = jsds.map(ds, Person::new, Person.class);
        assertEquals("foo", mappedDs.first().getName());
    }
}