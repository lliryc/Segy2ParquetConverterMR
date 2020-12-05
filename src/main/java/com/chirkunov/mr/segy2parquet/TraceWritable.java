/**
 * Custom writable object for trace record (header, data samples)
 * @author Kirill Chirkunov (https://github.com/lliryc)
 */
package com.chirkunov.mr.segy2parquet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Custom writable implementation for a seismic trace
 */
public class TraceWritable implements Writable {

    private TraceHeaderWritable traceHeader;
    private DoubleWritable[] traceData;

    //default constructor for (de)serialization
    public TraceWritable() {
        traceHeader = new TraceHeaderWritable();
        traceData = new DoubleWritable[0];
    }

    /**
     * Serialize TraceWritable
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        traceHeader.write(dataOutput);
        dataOutput.writeInt(traceData.length);
        for(int i=0; i<traceData.length; i++){
            traceData[i].write(dataOutput);
        }
    }

    /**
     * Deserialize TraceWritable
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        traceHeader.readFields(dataInput);
        int size = dataInput.readInt();
        traceData = new DoubleWritable[size];
        for(int i=0; i<traceData.length; i++){
            double v = dataInput.readDouble();
            traceData[i] = new DoubleWritable(v);
        }
    }

    /**
     * Get a trace header info
     * @return
     */
    public TraceHeaderWritable getTraceHeader() {
        return traceHeader;
    }

    /**
     * Set a trace header info
     * @param traceHeader
     */
    public void setTraceHeader(TraceHeaderWritable traceHeader) {
        this.traceHeader = traceHeader;
    }

    /**
     * Returns a DoubleWritable array of data samples
     * @return
     */
    public DoubleWritable[] getTraceData(){
        return traceData;
    }

    /**
     * Returns byte array with trace data samples
     * @return
     */
    public byte[] getTraceDataBytes(){
        int bufSize =  traceData.length * Double.BYTES;
        byte[] buffer = new byte[bufSize];
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        for(int i=0; i<traceData.length;i++){
            bb.putDouble(traceData[i].get());
        }
        return bb.array();
    }

    /**
     * Returns a double array with trace data samples
     * @return
     */
    public double[] getTraceDataDouble(){
        double[] val = new double[traceData.length];
        for(int i  = 0; i < traceData.length; i++){
            val[i] = traceData[i].get();
        }
        return val;
    }

    /**
     * Set a DoubleWritable array with trace data samples
     * @return
     */
    public void setTraceData(DoubleWritable[] traceData){
        this.traceData = traceData;
    }

    /**
     * Initialize TraceWritable from byte array, given a number format and data samples per trace
     * @param traceBytes trace byte array
     * @param nFmt SEGY number format
     * @param nSamples data samples per trace
     * @throws IOException
     */
    public void set(byte[] traceBytes, int nFmt, int nSamples) throws IOException {

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(traceBytes));

        byte[] traceHeaderBytes = new byte[SEGYInputFormat.TRACE_HEADER_SIZE];

        dis.read(traceHeaderBytes);

        traceHeader.fromBytes(traceHeaderBytes);
        traceData = new DoubleWritable[nSamples];

        for(int i = 0; i < nSamples; i++){
            traceData[i] = new DoubleWritable(NumFormatUtil.readFrom(nFmt, dis));
        }
    }

    /**
     * String representation
     * @return
     */
    @Override
    public String toString() {
        return traceHeader.toString() + ", TraceData[...]";
    }
}
