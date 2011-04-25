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
    public static final ConversionUtils instance = new ConversionUtils();

    private ObjectConverter os;

    public ConversionUtils() {
        try{
//            os = new Hessian2ObjectConverter();
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
        int n = 0;
        for (int i = 0; i < 4; i++) {
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
}
