package com.chirkunov.mr.segy2parquet;

import java.io.DataInputStream;
import java.io.IOException;

public class NumFormatUtil {
    public static int numBytesByFormat(short format) throws IllegalArgumentException{
        int res = 4; // by default
        switch (format) {
            case(1):
                res = 4; // IBM Floating point
                break;
            case(2):
                res = 4; // two's complement integer
                break;
            case (3):
                res = 2; // two's complement integer
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
                res = 1;
                break;
            default:
                res = 4;
                break;
        }
        return res;
    }
    public static double readFrom(int format, DataInputStream dis) throws IOException, IllegalArgumentException{
        int res = 4; // by default
        switch (format) {
            case(1):
                return floatFromBytes(dis); // IBM Floating point
            case(2):
                return dis.readInt();
            case (3):
                return dis.readShort();
            case (4):
                throw new IllegalArgumentException("Not supported"); // fixed-point with gain (obsolete)
            case (5):
                return dis.readFloat(); // 4-byte IEEE floating point
            case(6):
                throw new IllegalArgumentException("Not supported");
            case(7):
                throw new IllegalArgumentException("Not supported");
            case(8):
                return dis.read();
            default:
                return dis.readInt();
        }
    }

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
