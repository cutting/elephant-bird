package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Base class for Avro outputformats. */
public abstract class AvroOutputFormat<K, V> extends FileOutputFormat<K, V> {

  /** The file name extension for avro data files. */
  public final static String EXT = ".avro";

}
