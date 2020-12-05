/**
 * GroupWriteSupport implementation, according to Cloudera example
 * This class is responsible for writing Protobuf objects to Parquet file
 * See also: @see <a href="https://docs.cloudera.com/documentation/enterprise/5-4-x/topics/cdh_ig_parquet.html#parquet_mapreduce">https://docs.cloudera.com/documentation/enterprise/5-4-x/topics/cdh_ig_parquet.html#parquet_mapreduce</a>
 * @author Kirill Chirkunov (https://github.com/lliryc)
 */

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
import org.apache.parquet.schema.MessageTypeParser;

/**
 * Implementation of TraceGroupWriteSupport
 */
public class TraceGroupWriteSupport extends WriteSupport<Group> {

    public static final String PARQUET_TRACE_SCHEMA = "parquet.trace.schema";
    private static String writeSchema = "message Trace {\n" +
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

    private MessageType schema;
    private GroupWriter groupWriter;
    private Map<String, String> extraMetaData;

    public TraceGroupWriteSupport() {
        // Protobuf description of an exported Parquet rows
        // @see <a href="https://developers.google.com/protocol-buffers/docs/proto">https://developers.google.com/protocol-buffers/docs/proto</a>
        MessageType messageType = MessageTypeParser.parseMessageType(writeSchema);
        this.schema = messageType;
        this.extraMetaData = new HashMap<String, String>();
    }

    public static MessageType getSchema(){
        return MessageTypeParser.parseMessageType(writeSchema);
    }

    @Override
    public String getName() {
        return "trace";
    }

    @Override
    public org.apache.parquet.hadoop.api.WriteSupport.WriteContext init(Configuration configuration) {
        // if present, prefer the schema passed to the constructor
        return new WriteContext(MessageTypeParser.parseMessageType(writeSchema), this.extraMetaData);
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

