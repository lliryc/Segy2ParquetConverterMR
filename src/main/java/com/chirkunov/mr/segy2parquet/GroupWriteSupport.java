package com.chirkunov.mr.segy2parquet;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

public class GroupWriteSupport extends WriteSupport<Group> {

    public static final String PARQUET_TRACE_SCHEMA = "parquet.trace.schema";

    public static void setSchema(MessageType schema, Configuration configuration) {
        configuration.set(PARQUET_TRACE_SCHEMA, schema.toString());
    }

    public static MessageType getSchema(Configuration configuration) {
        return parseMessageType(Objects.requireNonNull(configuration.get(PARQUET_TRACE_SCHEMA), PARQUET_TRACE_SCHEMA));
    }

    private MessageType schema;
    private GroupWriter groupWriter;
    private Map<String, String> extraMetaData;

    public GroupWriteSupport() {
        this(null, new HashMap<String, String>());
    }

    GroupWriteSupport(MessageType schema) {
        this(schema, new HashMap<String, String>());
    }

    GroupWriteSupport(MessageType schema, Map<String, String> extraMetaData) {
        this.schema = schema;
        this.extraMetaData = extraMetaData;
    }

    @Override
    public String getName() {
        return "trace";
    }

    @Override
    public org.apache.parquet.hadoop.api.WriteSupport.WriteContext init(Configuration configuration) {
        // if present, prefer the schema passed to the constructor
        if (schema == null) {
            schema = getSchema(configuration);
        }
        return new WriteContext(schema, this.extraMetaData);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        groupWriter = new GroupWriter(recordConsumer, schema);
    }

    @Override
    public void write(Group record) {
        groupWriter.write(record);
    }

}

