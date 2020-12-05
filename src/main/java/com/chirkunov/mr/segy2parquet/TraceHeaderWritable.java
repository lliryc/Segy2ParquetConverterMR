/**
 * Custom writable key for getting trace record header
 * It contains base information about a trace, such as:
 * trace unique id (within SEGY),
 * field record number id,
 * a distance from a source to a receiver,
 * X,Y coordinates of a source,
 * a sample interval in ms,
 * iline and xline ids,
 * @author Kirill Chirkunov (https://github.com/lliryc)
 */

package com.chirkunov.mr.segy2parquet;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Custom writable key for getting trace record header info
 */
public class TraceHeaderWritable implements WritableComparable {

    public TraceHeaderWritable(){
        fieldRecordNumberID = 0;
        distSRG = 0;
        srcX = 0;
        srcY = 0;
        sI = 0;
        ilineID = 0;
        xlineID = 0;
    }
    // byte order starts with (1 ...)
    //(5-8) Trace id through all file
    private final int TRACEID_OFFSET = 4;
    private final int TRACEID_SIZE = 4;

    private int traceID;

    /**
     * Returns a unique trace id (within SEGY file)
     * @return traceId
     */
    public int getTraceID(){
        return traceID;
    }


    //(9-12) Original field record number
    private final int FRN_OFFSET = 8;
    private final int FRN_SIZE = 4;

    private int fieldRecordNumberID;

    /**
     * Returns an id of a field record number
     * @return fieldRecordNumberID
     */
    public int getFieldRecordNumberID(){
        return fieldRecordNumberID;
    }

    //(37-40) Distance from center of the source point to the center of the receiver group
    private final int DST_SRG_OFFSET = 36;
    private final int DST_SRG_SIZE = 4;

    private int distSRG;

    /**
     * Returns a distance between a source and a receiver
     * Please notice it could be negative
     * @return distSRG
     */
    public int getDistSRG(){
        return distSRG;
    }

    //(73-76) Source X Coordinate
    private final int SRCX_OFFSET = 72;
    private final int SRCX_SIZE = 4;

    private int srcX;

    /**
     * Returns a X-coordinate of a source
     * @return srcX
     */
    public int getSrcX(){
        return srcX;
    }

    //(77-80) Source Y Coordinate
    private final int SRCY_OFFSET = 76;
    private final int SRCY_SIZE = 4;
    private int srcY;
    /**
     * Returns a Y-coordinate of a source
     * @return srcY
     */
    public int getSrcY(){
        return srcY;
    }

    //(117-118) Sample interval for this trace in ms
    private final int SI_OFFSET = 116;
    private final int SI_SIZE = 2;
    private short sI;

    /**
     * Returns a sample interval
     * @return sI
     */
    public short getSI(){
        return sI;
    }

    //(189-192) inline number
    private final int IL_OFFSET = 188;
    private final int IL_SIZE = 4;
    private int ilineID;
    /**
     * Returns an inline id
     * @return ilineID
     */
    public int getILineID(){
        return ilineID;
    }

    //(193-196) xline number
    private final int XL_OFFSET = 192;
    private final int XL_SIZE = 4;
    private int xlineID;
    /**
     * Returns an xline id
     * @return xlineID
     */
    public int getXLineID(){
        return xlineID;
    }

    /**
     * Initialize TraceHeaderWritable from bytes array (following to SEGY spec)
     * @param traceHeaderBytes bytes array
     */
    public void fromBytes(byte[]traceHeaderBytes){
        ByteBuffer bb = ByteBuffer.wrap(traceHeaderBytes);
        bb.position(TRACEID_OFFSET);
        traceID = bb.getInt();
        bb.position(FRN_OFFSET);
        fieldRecordNumberID = bb.getInt();
        bb.position(DST_SRG_OFFSET);
        distSRG = bb.getInt();
        bb.position(SRCX_OFFSET);
        srcX = bb.getInt();
        bb.position(SRCY_OFFSET);
        srcY = bb.getInt();
        bb.position(SI_OFFSET);
        sI = bb.getShort();
        bb.position(IL_OFFSET);
        ilineID = bb.getInt();
        bb.position(XL_OFFSET);
        xlineID = bb.getInt();
    }

    /**
     * Serialize TraceHeaderWritable
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(traceID);
        dataOutput.writeInt(fieldRecordNumberID);
        dataOutput.writeInt(distSRG);
        dataOutput.writeInt(srcX);
        dataOutput.writeInt(srcY);
        dataOutput.writeShort(sI);
        dataOutput.writeInt(ilineID);
        dataOutput.writeInt(xlineID);
    }

    /**
     * Deserialize TraceHeaderWritable
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        traceID = dataInput.readInt();
        fieldRecordNumberID = dataInput.readInt();
        distSRG = dataInput.readInt();
        srcX = dataInput.readInt();
        srcY = dataInput.readInt();
        sI = dataInput.readShort();
        ilineID = dataInput.readInt();
        xlineID = dataInput.readInt();
    }

    /**
     * Initialize TraceHeaderWritable from other instance
     * @param tw
     */
    public void set(TraceHeaderWritable tw){
        traceID = tw.traceID;
        fieldRecordNumberID = tw.getFieldRecordNumberID();
        distSRG = tw.getDistSRG();
        srcX = tw.getSrcX();
        srcY = tw.getSrcY();
        sI = tw.getSI();
        ilineID = tw.getILineID();
        xlineID = tw.getXLineID();
    }

    /**
     * Compare TraceWritable key with other TraceWritable key using traceID
     * @param o: TraceHeaderWritable instance to compare
     * @return negative if less, zero if is equal to, positive if greater
     */
    @Override
    public int compareTo(Object o) {
        TraceHeaderWritable tw = (TraceHeaderWritable)o;
        return traceID - tw.traceID;
    }

    /**
     * String representation of trace header
     * @return
     */
    @Override
    public String toString() {
        return String.format("TraceHeader(traceId=%d)", traceID);
    }

}
