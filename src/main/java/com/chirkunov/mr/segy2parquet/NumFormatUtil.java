/**
 * NumFormatUtil: Utility class to perform read operations on trace data samples
 * It follows SEGY (rev 1, rev2) specification and supports
 * IBM hexadecimal floating point (@see <a href="https://en.wikipedia.org/wiki/IBM_hexadecimal_floating_point">https://en.wikipedia.org/wiki/IBM_hexadecimal_floating_point</a>),
 * Two's complement integer,
 * 4-bytes IEEE floating point
 * Please notice that fixed-point number with gain is not supported
 * For more info please read SEGY specification: @see <a href="https://seg.org/Portals/0/SEG/News%20and%20Resources/Technical%20Standards/seg_y_rev2_0-mar2017.pdf">https://seg.org/Portals/0/SEG/News%20and%20Resources/Technical%20Standards/seg_y_rev2_0-mar2017.pdf</a>
 * @author Kirill Chirkunov (https://github.com/lliryc)
 */
package com.chirkunov.mr.segy2parquet;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Utility class for SEGY read numbers operations
 */
public class NumFormatUtil {
    /**
     * Following to SEGY specification, method takes format code as input and returns size of data sample in bytes
     * @param SEGY number format located at (3223,) offset
     * @return number of bytes reserved for one data sample
     * @throws IllegalArgumentException
     */
    public static int numBytesByFormat(short format) throws IllegalArgumentException{
        int res;
        switch (format) {
            case(1):
                res = 4; // IBM hexadecimal floating point
                break;
            case(2):
                res = 4; // two's complement integer
                break;
            case (3):
                res = 2; // two's complement short
                break;
            case (4):
                res = 4; // fixed-point with gain (obsolete)
                break;
            case (5):
                res = 4; // 4-byte IEEE floating point
                break;
            case(6):
                throw new IllegalArgumentException("Not supported");
            case(7):
                throw new IllegalArgumentException("Not supported");
            case(8):
                res = 1; // just a byte value
                break;
            default:
                res = 4;
                break;
        }
        return res;
    }

    /**
     * Reads next trace data sample from stream, according to the number format code
     * @param format SEGY number format specification
     * @param dis Binary stream with trace data samples
     * @return
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public static double readFrom(int format, DataInputStream dis) throws IOException, IllegalArgumentException{
        int res = 4; // by default
        switch (format) {
            case(1):
                return floatFromBytes(dis); // IBM hexadecimal floating point
            case(2):
                return dis.readInt(); // two's complement integer
            case (3):
                return dis.readShort(); // two's complement short
            case (4):
                throw new IllegalArgumentException("Not supported"); // fixed-point with gain (obsolete)
            case (5):
                return dis.readFloat(); // 4-byte IEEE floating point
            case(6):
                throw new IllegalArgumentException("Not supported");
            case(7):
                throw new IllegalArgumentException("Not supported");
            case(8):
                return dis.read(); // read one byte
            default:
                return dis.readInt();
        }
    }

    /**
     * Ad hoc method to read IBM hexadecimal floating point from binary stream
     * See also the StackOverflow discussion regarding this topic: @see <a href="https://stackoverflow.com/questions/34565189/java-ieee-754-float-to-ibm-float-byte4-conversion">https://stackoverflow.com/questions/34565189/java-ieee-754-float-to-ibm-float-byte4-conversion</a>
     * @param dis Binary stream
     * @return IBM hexadecimal floating point
     * @throws IllegalArgumentException
     * @throws IOException
     */
    private static float floatFromBytes(DataInputStream dis) throws IllegalArgumentException, IOException {
        byte a =  dis.readByte();
        byte b = dis.readByte();
        byte c = dis.readByte();
        byte d = dis.readByte();
        int sgn, mant, exp;
        mant = ( b &0xFF) << 16 | (c & 0xFF ) << 8 | ( d & 0xFF);
        if (mant == 0) return 0.0f;
        sgn = -(((a & 128) >> 6) - 1);
        exp = (a & 127) - 64;
        return (float) (sgn * Math.pow(16.0, exp - 6) * mant);
    }
}
