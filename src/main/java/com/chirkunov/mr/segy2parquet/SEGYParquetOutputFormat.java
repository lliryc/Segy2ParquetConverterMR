package com.chirkunov.mr.segy2parquet;

import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.schema.MessageType;

public class SEGYParquetOutputFormat extends ParquetOutputFormat<Group> {

    /**
     * set the schema being written to the job conf
     * @param job a job
     * @param schema the schema of the data
     */
    public static void setSchema(Job job, MessageType schema) {
        GroupWriteSupport.setSchema(schema, ContextUtil.getConfiguration(job));
    }

    /**
     * retrieve the schema from the conf
     * @param job a job
     * @return the schema
     */
    public static MessageType getSchema(Job job) {
        return GroupWriteSupport.getSchema(ContextUtil.getConfiguration(job));
    }

    public SEGYParquetOutputFormat() {
        super(new GroupWriteSupport());
    }
}