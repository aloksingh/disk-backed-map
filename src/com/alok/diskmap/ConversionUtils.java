/*
 * Copyright 2009 Alok Singh
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.alok.diskmap;

import com.alok.diskmap.utils.DefaultObjectConverter;
import com.alok.diskmap.utils.ObjectConverter;

import java.io.Serializable;

public class ConversionUtils {
    private static final int poly = 0x1021;

    private static final int[] crcTable = new int[256];

    static {
        // initialise scrambler table
        for (int i = 0; i < 256; i++) {
            int fcs = 0;
            int d = i << 8;
            for (int k = 0; k < 8; k++) {
                if (((fcs ^ d) & 0x8000) != 0) {
                    fcs = (fcs << 1) ^ poly;
                } else {
                    fcs = (fcs << 1);
                }
                d <<= 1;
                fcs &= 0xffff;
            }
            crcTable[i] = fcs;
        }
    }

    public static final ConversionUtils instance = new ConversionUtils();

    private ObjectConverter os;

    public ConversionUtils() {
        try{
            os = new DefaultObjectConverter();
        }catch(Exception e){
            System.err.println("Unable to create hessian object convertor, using the default convertor.");
            os = new DefaultObjectConverter();
        }
    }

    public byte[] intToBytes(int n) {
        byte[] b = new byte[4];
        for (int i = 0; i < b.length; i++) {
            b[3 - i] = (byte) (n >>> (i * 8));
        }
        return b;
    }

    public byte[] longToBytes(long n) {
        byte[] b = new byte[8];
        for (int i = 0; i < b.length; i++) {
            b[b.length - 1 - i] = (byte) (n >>> (i * 8));
        }
        return b;
    }

    public int byteToInt(byte[] b) {
        return byteToInt(b, 0);
    }
    public int byteToInt(byte[] b, int offset) {
        int n = 0;
        for (int i = offset; i < offset + 4; i++) {
            n <<= 8;
            n ^= (int) b[i] & 0xFF;
        }
        return n;
    }

    public long byteToLong(byte[] b) {
        long n = 0;
        for (int i = 0; i < 8; i++) {
            n <<= 8;
            n ^= (long) b[i] & 0xFF;
        }
        return n;
    }

    public byte[] serialize(Serializable object) throws Exception {
        return os.serialize(object);
    }

    public <T> T deserialize(byte[] buffer) {
        return (T) os.deserialize(buffer);
    }

    public byte[] shortToBytes(int n) {
        return shortToBytes((short) n);
    }
    public byte[] shortToBytes(short n) {
        byte[] b = new byte[2];
        b[1] = (byte) (n >>> 0);
        b[0] = (byte) (n >>> 8);
        return b;
    }

    public short byteToShort(byte[] b) {
        return byteToShort(b, 0);
    }
    public short byteToShort(byte[] b, int offset) {
        short n = 0;
        for (int i = offset; i < offset + 2; i++) {
            n <<= 8;
            n ^= (int) b[i] & 0xFF;
        }
        return n;
    }

    public static short crc16(byte[] ba) {
        int work = 0xffff;
        for (byte b : ba) {
            work = (crcTable[(b ^ (work >>> 8)) & 0xff] ^ (work << 8)) &
                    0xffff;
        }
        return (short) work;
    }
}
