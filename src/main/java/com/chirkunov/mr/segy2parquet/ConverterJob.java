package com.chirkunov.mr.segy2parquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.example.data.simple.*;
import java.util.UUID;

public class ConverterJob extends Configured implements Tool {

    public ConverterJob(){
        String writeSchema = "message Trace {\n" +
                "required int32 traceID = 1;\n" +
                "required int32 fieldRecordNumberID = 2;\n" +
                "required int32 distSRG = 3;\n" +
                "required int32 srcX = 4;\n" +
                "required int32 srcY = 5;\n" +
                "required int32 sI = 6;\n" +
                "required int32 ilineID = 7;\n" +
                "required int32 xlineID = 8;\n" +
                "repeated double traceData = 9;\n" +
                "}";

        messageType = MessageTypeParser.parseMessageType(writeSchema);
    }

    private MessageType messageType;

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

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
        job.setMapOutputKeyClass(Void.class);
        job.setMapOutputValueClass(Group.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(SEGYInputFormat.class);

        SEGYParquetOutputFormat.setSchema( job, messageType);

        GroupWriteSupport.setSchema(messageType, getConf());

        job.setOutputFormatClass(ParquetOutputFormat.class);

        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setCompressOutput(job, true);
        ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);

        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
    }

    public static class MapClass extends Mapper<TraceHeaderWritable, TraceWritable, Void, Group> {

        @Override
        protected void map(TraceHeaderWritable key, TraceWritable tw, Context context) throws IOException, InterruptedException {
            String writeSchema = "message Trace {\n" +
                    "required int32 traceID = 1;\n" +
                    "required int32 fieldRecordNumberID = 2;\n" +
                    "required int32 distSRG = 3;\n" +
                    "required int32 srcX = 4;\n" +
                    "required int32 srcY = 5;\n" +
                    "required int32 sI = 6;\n" +
                    "required int32 ilineID = 7;\n" +
                    "required int32 xlineID = 8;\n" +
                    "repeated double traceData = 9;\n" +
                    "}";
            MessageType mt = MessageTypeParser.parseMessageType(writeSchema);
            Group group = new SimpleGroup(mt);
            TraceHeaderWritable thw = key;
            group.add(0, thw.getTraceID());
            group.add(1, thw.getFieldRecordNumberID());
            group.add(2, thw.getDistSRG());
            group.add(3, thw.getSrcX());
            group.add(4, thw.getSrcY());
            group.add(5, (int) thw.getSI());
            group.add(6, thw.getILineID());
            group.add(7, thw.getXLineID());
            for(double item:tw.getTraceDataDouble()){
                group.add(8, item);
            }
            context.write(null, group);

        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ConverterJob(), args);
        System.exit(res);
    }
}

