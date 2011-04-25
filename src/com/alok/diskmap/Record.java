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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Record implements Comparable<Record>{
    private static final ConversionUtils util = new ConversionUtils();
    private static final Logger logger = Logger.getLogger(Record.class.getName());

    private final boolean DEBUG = false;
    public static final int ACTIVE = 1;
    public static final int DELETED = 2;
    public static final int EMPTY = 4;

    private int flag;
    private int hash;
    private int keySize;
    private byte[] key;
    private int valueSize;
    private byte[] value;
    private long location = -1;

    public Record() {
    }

    public Record(byte[] key, byte[] value, int flag, int hash, long location) {
        this.flag = flag;
        this.hash = hash;
        this.keySize = key.length;
        this.key = key;
        this.valueSize = value.length;
        this.value = value;
        this.location = location;
    }

    public Record(Record r, long newLocation) {
        this(r.getKey(), r.getValue(), r.getFlag(), r.getHash(), newLocation);
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public void write(DataOutput index) throws IOException {
        writeIndex(index);
        writeDate(index);
    }

    public void read(DataInput index) throws IOException {
        readIndex(index);
        readData(index);
    }

    public void writeIndex(DataOutput index) throws IOException {
        index.write(util.intToBytes(flag));
        index.write(0);
        index.write(util.intToBytes(hash));
        index.write(0);
        index.write(util.longToBytes(location));
        index.write(0);
    }

    public void readIndex(DataInput index) throws IOException {
        setFlag(readInt(index));
        index.readByte();
        this.hash = readInt(index);
        index.readByte();
        this.location = readLong(index);
        index.readByte();
    }

    public void writeDate(DataOutput index) throws IOException {
        index.write(util.intToBytes(keySize));
        index.write(0);
        index.write(util.intToBytes(valueSize));
        index.write(0);
        index.write(key);
        index.write(0);
        index.write(value);
        index.write(0);
    }

    public void readData(DataInput index) throws IOException {
        this.keySize = readInt(index);
        index.readByte();
        this.valueSize = readInt(index);
        index.readByte();
        if(DEBUG){
            logger.log(Level.SEVERE, String.format("keySize[%d], valueSize[%d]", keySize, valueSize));
            if(keySize > 1000){
                logger.log(Level.SEVERE, "Large key");
            }
        }
        this.key = new byte[keySize];
        this.value = new byte[valueSize];
        index.readFully(key);
        index.readByte();
        index.readFully(value);
        index.readByte();
    }

    private int readInt(DataInput file) throws IOException {
        byte[] buffer = new byte[4];
        file.readFully(buffer);
        return util.byteToInt(buffer);
    }

    private long readLong(DataInput file) throws IOException {
        byte[] buffer = new byte[8];
        file.readFully(buffer);
        return util.byteToLong(buffer);
    }

    public boolean equals(Object other) {
        if (other != null && !(other instanceof Record)) {
            return false;
        }
        Record that = (Record) other;
        if (this.flag != that.flag) {
            return false;
        }
        if (this.hash != that.hash) {
            return false;
        }
        if (this.keySize != that.keySize) {
            return false;
        }
        if (this.valueSize != that.valueSize) {
            return false;
        }

        if (this.location != that.location) {
            return false;
        }

        if (this.key.length != that.key.length) {
            return false;
        }
        for (int i = 0; i < this.key.length; i++) {
            if (this.key[i] != that.key[i]) {
                return false;
            }
        }

        if (this.value.length != that.value.length) {
            return false;
        }
        for (int i = 0; i < this.value.length; i++) {
            if (this.value[i] != that.value[i]) {
                return false;
            }
        }
        return true;
    }

    public int getHash() {
        return hash;
    }

    public int getFlag() {
        return flag;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getLocation() {
        return location;
    }

    public int size() {
        return keySize + valueSize + (4 * 4) + 8;// int fields + long fields
    }

    @Override
    public String toString() {
        return "Record{" +
                "key=" + util.deserialize(key) +
                "value =" + util.deserialize(value) +
                "keySize =" + keySize+
                "valueSize =" + valueSize +
                ", hash=" + hash +
                ", flag=" + flag +
                ", location=" + location +
                '}';
    }

    public void setLocation(long location) {
        this.location = location;
    }

    @Override
    public int compareTo(Record o) {
        return location > o.location ? 1 : (location < o.location ? -1 : 0);
    }
}
