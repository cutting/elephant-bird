package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.avro.protobuf.ProtobufDatumWriter;

/**
 * This is the class for Avro protocol buffer based output format.
 * Data is written as Avro data files using the snappy codec.
 */

public class AvroProtobufOutputFormat<M extends Message>
  extends AvroOutputFormat<M, ProtobufWritable<M>> {

  protected TypeRef<M> type;

  protected void setTypeRef(TypeRef<M> typeRef) { type = typeRef; }

  public AvroProtobufOutputFormat() {}

  public AvroProtobufOutputFormat(TypeRef<M> typeRef) { type = typeRef; }

  /**
   * Returns {@link AvroProtobufOutputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object for this generic class based on protoClass
   */
  @SuppressWarnings("unchecked")
  public static <M extends Message> Class<AvroProtobufOutputFormat>
    getOutputFormatClass(Class<M> protoClass, Configuration jobConf) {

    Protobufs.setClassConf(jobConf, AvroProtobufOutputFormat.class, protoClass);
    return AvroProtobufOutputFormat.class;
  }

  @Override
  public RecordWriter<M, ProtobufWritable<M>>
    getRecordWriter(TaskAttemptContext job)
    throws IOException, InterruptedException {
    if (type == null)
      type = Protobufs.getTypeRef(job.getConfiguration(),
                                  AvroProtobufOutputFormat.class);

    Schema schema = ProtobufData.get().getSchema(type.getRawClass());

    final DataFileWriter<M> writer =
      new DataFileWriter<M>(new ProtobufDatumWriter<M>(schema));
    
    writer.setCodec(CodecFactory.snappyCodec());

    Path path = getDefaultWorkFile(job, EXT);

    writer.create(schema,
                  path.getFileSystem(job.getConfiguration()).create(path));

    return new RecordWriter<M, ProtobufWritable<M>>() {
      public void write(M ignore, ProtobufWritable<M> wrapper)
        throws IOException {
        writer.append(wrapper.get());
      }
      public void close(TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {
        writer.close();
      }
    };

  }
}
