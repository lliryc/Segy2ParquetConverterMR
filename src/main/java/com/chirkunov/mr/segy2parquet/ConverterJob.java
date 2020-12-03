package com.chirkunov.mr.segy2parquet;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoWriteSupport;
import org.apache.parquet.schema.MessageTypeParser;


import java.util.UUID;

public class ConverterJob extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        //conf.set("fs.defaultFS", "local");
        conf.set("fs.defaultFS", "file:///");

        conf.set("mapreduce.job.maps","1");
        conf.set("mapreduce.job.reduces","1");

        conf.set("key.value.separator.in.input.line", ",");
        Job job = Job.getInstance(conf, "patent references collect");
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        Path outSub = new Path(UUID.randomUUID().toString());
        out = Path.mergePaths(out, outSub);
        job.setJarByClass(ConverterJob.class);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setMapperClass(ConverterJob.MapClass.class);
        job.setMapOutputKeyClass(TraceHeaderWritable.class);
        job.setMapOutputValueClass(TraceWritable.class);
        job.setInputFormatClass(SEGYInputFormat.class);
        /*
        String writeSchema = "message SEGY {\n" +
                "required bytes TraceHeader;\n" +
                "required repeated double TraceData;\n" +
                "}";
        SEGYParquetOutputFormat.setSchema(
                job,
                MessageTypeParser.parseMessageType(writeSchema));
        */
        /* just temporary solution */
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(TraceHeaderWritable.class);
        job.setOutputValueClass(TraceWritable.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job,
                SequenceFile.CompressionType.BLOCK);

        /*
        job.setOutputFormatClass(ParquetOutputFormat.class);

        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setCompressOutput(job, true);
        ParquetOutputFormat.setWriteSupportClass(job, ProtoWriteSupport.class);
        */


        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
    }

    public static class MapClass extends Mapper<TraceHeaderWritable, TraceWritable, TraceHeaderWritable, TraceWritable> {
        @Override
        protected void map(TraceHeaderWritable key, TraceWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ConverterJob(), args);
        System.exit(res);
    }
}
