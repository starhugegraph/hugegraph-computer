package com.baidu.hugegraph.computer.core.dataparser;

import com.baidu.hugegraph.computer.core.util.CoderUtil;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


public class DataParser {
    
    public static boolean byte2boolean(byte[] b, int offset) {
        return b[offset] == 1;
    }

    public static byte[] int2byte(int value) {
        return new byte[] {
                (byte)value,
                (byte)(value >>> 8),
                (byte)(value >>> 16),
                (byte)(value >>> 24)};
    }

    public static int byte2int(byte[] b, int offset) {  
        return   b[offset] & 0xFF |  
                    (b[offset + 1] & 0xFF) << 8 |  
                    (b[offset + 2] & 0xFF) << 16 |  
                    (b[offset + 3] & 0xFF) << 24;  
    }  

    public static long byte2long(byte[] b, int offset) {
        long l; 
        l =  b[offset + 0] & 0xFFL |  
            (b[offset + 1] & 0xFFL) << 8 |  
            (b[offset + 2] & 0xFFL) << 16 |  
            (b[offset + 3] & 0xFFL) << 24 |
            (b[offset + 4] & 0xFFL) << 32 |  
            (b[offset + 5] & 0xFFL) << 40 |  
            (b[offset + 6] & 0xFFL) << 48 |
            (b[offset + 7] & 0xFFL) << 56;
        return l;
    }  

    public static float byte2float(byte[] b, int offset) {    
        int l;                                             
        l = b[offset] & 0xFF |  
            (b[offset + 1] & 0xFF) << 8 |  
            (b[offset + 2] & 0xFF) << 16 |  
            (b[offset + 3] & 0xFF) << 24;                   
        return Float.intBitsToFloat(l);                    
    }  

    public static double byte2double(byte[] b, int offset) {    
        long l;                                             
        l = b[offset + 0] & 0xFFL |  
            (b[offset + 1] & 0xFFL) << 8 |  
            (b[offset + 2] & 0xFFL) << 16 |  
            (b[offset + 3] & 0xFFL) << 24 |
            (b[offset + 4] & 0xFFL) << 32 |  
            (b[offset + 5] & 0xFFL) << 40 |  
            (b[offset + 6] & 0xFFL) << 48 |
            (b[offset + 7] & 0xFFL) << 56;

        return Double.longBitsToDouble(l);                    
    }  
    
    public static long[] parseVLong(byte[] data, int off) {
        long[] result = new long[2];
        byte leading = data[off];

        long value = leading & 0x7fL;
        if (leading >= 0) {
            assert (leading & 0x80) == 0;
            result[0] = value;
            result[1] = 1;
            return result;
        }

        int i = 1;
        for (; i < 10; i++) {
            byte b = data[off + i];
            if (b >= 0) {
                value = b | (value << 7);
                result[0] = value;
                result[1] = i + 1;
                break;
            } else {
                value = (b & 0x7f) | (value << 7);
                result[0] = value;
                result[1] = i + 1;
            }
        }
        return result;
    }

    public static int[] parseVInt(byte[] data, int off) {
        int[] result = new int[2];
        byte leading = data[off];
        int value = leading & 0x7f;
        if (leading >= 0) {
            assert (leading & 0x80) == 0;
            result[0] = value;
            result[1] = 1;
            return result;
        }
        int i = 1;
        for (; i < 5; i++) {
            byte b = data[off + i];
            if (b >= 0) {
                value = b | (value << 7);
                result[0] = value;
                result[1] = i + 1;
                break;
            } else {
                value = (b & 0x7f) | (value << 7);
                result[0] = value;
                result[1] = i + 1;
            }
        }
        return result;
    }

    public static Pair<String, Integer> parseUTF(byte[] data, int offset) {
        int position = offset;
        
        int[] vint = DataParser.parseVInt(data, position);
        int length = vint[0];
        int shift = vint[1];
        position += 1;

        String str = CoderUtil.decode(data, position, length);
        shift += length;

        Pair<String, Integer> pair = new ImmutablePair<>(str, shift);
        return pair;
    }

    public static byte[] parseKey(byte[] data) {
        int keylength = DataParser.byte2int(data, 0);
        byte[] result = new byte[keylength];
        System.arraycopy(data, 4, result, 0, keylength);
        return result;
    }

    public static byte[] parseValue(byte[] data) {
        int keylength = DataParser.byte2int(data, 0);
        int valuelength = DataParser.byte2int(data, keylength + 4);
        byte[] result = new byte[valuelength];
        System.arraycopy(data, keylength + 8, result, 0, valuelength);
        return result;
    }
}
