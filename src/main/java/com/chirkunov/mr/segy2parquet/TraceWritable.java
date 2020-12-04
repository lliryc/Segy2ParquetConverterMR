package com.chirkunov.mr.segy2parquet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;

import java.io.*;
import java.nio.ByteBuffer;

public class TraceWritable implements Writable{

    private TraceHeaderWritable traceHeader;
    private DoubleWritable[] traceData;

    //default constructor for (de)serialization
    public TraceWritable() {
        traceHeader = new TraceHeaderWritable();
        traceData = new DoubleWritable[0];
    }

    public void write(DataOutput dataOutput) throws IOException {
        traceHeader.write(dataOutput);
        dataOutput.writeInt(traceData.length);
        for(int i=0; i<traceData.length; i++){
            traceData[i].write(dataOutput);
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        traceHeader.readFields(dataInput);
        int size = dataInput.readInt();
        traceData = new DoubleWritable[size];
        for(int i=0; i<traceData.length; i++){
            double v = dataInput.readDouble();
            traceData[i] = new DoubleWritable(v);
        }
    }

    public TraceHeaderWritable getTraceHeader() {
        return traceHeader;
    }

    public void setTraceHeader(TraceHeaderWritable traceHeader) {
        this.traceHeader = traceHeader;
    }

    public DoubleWritable[] getTraceData(){
        return traceData;
    }

    public byte[] getTraceDataBytes(){
        int bufSize =  traceData.length * Double.BYTES;
        byte[] buffer = new byte[bufSize];
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        for(int i=0; i<traceData.length;i++){
            bb.putDouble(traceData[i].get());
        }
        return bb.array();
    }

    public double[] getTraceDataDouble(){
        double[] val = new double[traceData.length];
        for(int i  = 0; i < traceData.length; i++){
            val[i] = traceData[i].get();
        }
        return val;
    }

    public void setTraceData(DoubleWritable[] traceData){
        this.traceData = traceData;
    }

    public void set(byte[] traceBytes, int nBytes, int nFmt, int nSamples) throws IOException {

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(traceBytes));

        byte[] traceHeaderBytes = new byte[SEGYInputFormat.TRACE_HEADER_SIZE];

        dis.read(traceHeaderBytes);

        traceHeader.fromBytes(traceHeaderBytes);
        traceData = new DoubleWritable[nSamples];

        for(int i=0; i<nSamples; i++){
            traceData[i] = new DoubleWritable(NumFormatUtil.readFrom(nFmt, dis));
        }
    }

    @Override
    public String toString() {
        return traceHeader.toString() + ", TraceData[...]";
    }
}
