/**
 * Map-Reduce Job implementation to convert SEGY to Parquet
 * It consists of the nested Mapper which maps SEGY input to Parquet-compatible format,
 * based on Protobuf protocol, run method to configure and launch the job,
 * and main entry point for program
 * @author Kirill Chirkunov (https://github.com/lliryc)
 */
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

/**
 * MapReduce Job to convert SEGY to Parquet format
 */
public class ConverterJob extends Configured implements Tool {

    public ConverterJob(){
        // Protobuf description of an exported Parquet rows
        // @see <a href="https://developers.google.com/protocol-buffers/docs/proto">https://developers.google.com/protocol-buffers/docs/proto</a>
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
    // Parsed protobuf description of the Parquet row
    private MessageType messageType;
    // Setting name of the map tasks number per job
    private final String CONF_MAPREDUCE_JOB_MAPS =  "mapreduce.job.maps";
    // Setting name of the default filesystem (standard Hadoop setting)
    private final String CONF_FS_DFS =  "fs.defaultFS";


    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        //Uncomment this section for the debug purposes
        //conf.set(CONF_FS_DFS, "file:///");
        //conf.set(CONF_MAPREDUCE_JOB_MAPS,"1");

        Job job = Job.getInstance(conf, "Converting SEGY to Parguet");
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        Path outSub = new Path(UUID.randomUUID().toString());
        out = Path.mergePaths(out, outSub);
        job.setJarByClass(ConverterJob.class);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setMapperClass(ConverterJob.MapClass.class);
        // Default Parquet mapper maps (k,v) to (Void, Group) pair
        job.setMapOutputKeyClass(Void.class);
        job.setMapOutputValueClass(Group.class);

        job.setNumReduceTasks(0);
        job.setInputFormatClass(SEGYInputFormat.class);
        GroupWriteSupport.setSchema(messageType, getConf());
        job.setOutputFormatClass(ParquetOutputFormat.class);

        // Enable SNAPPY compression to make result parquet files more compact
        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setCompressOutput(job, true);

        ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);

        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
    }

    public static class MapClass extends Mapper<TraceHeaderWritable, TraceWritable, Void, Group> {

        @Override
        protected void map(TraceHeaderWritable key, TraceWritable tw, Context context) throws IOException, InterruptedException {
            // Protobuf Parquet row description
            // Mainly it corresponds to SEGY Trace format,
            // however trace data samples are stored in the Double type
            // (compromise between Int and Float types)
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
            // protobuf map order: (1->0), (2->1), (3->2) ...
            group.add(0, thw.getTraceID());
            group.add(1, thw.getFieldRecordNumberID());
            group.add(2, thw.getDistSRG());
            group.add(3, thw.getSrcX());
            group.add(4, thw.getSrcY());
            group.add(5, (int) thw.getSI());
            group.add(6, thw.getILineID());
            group.add(7, thw.getXLineID());
            // write an array of samples data
            for(double item:tw.getTraceDataDouble()){
                group.add(8, item);
            }
            context.write(null, group);

        }
    }

    /**
     * Main entry point to start ConverterJob
     * @param args: args[0] - job input folder (with SEGY files), args[1] - job output folder (for Parquet files)
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ConverterJob(), args);
        System.exit(res);
    }
}

