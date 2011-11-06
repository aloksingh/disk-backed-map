package com.alok.diskmap;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ZipMap implements Map<String, byte[]>{
    private byte[] data;
    private int length;
    private int dataSize;
    private int items;
    private ConversionUtils utils = ConversionUtils.instance;
    ReadWriteLock rwl = new ReentrantReadWriteLock();
    private static final int KEY_SIZE_LEN = 2;
    private static final int VALUE_SIZE_LEN = 2;
    private static final int SIZE_LEN = KEY_SIZE_LEN + VALUE_SIZE_LEN;
    private static final int CRC_LEN = 2;

    public ZipMap(){
        init();
    }

    private void init() {
        this.data = new byte[1024];
        this.length = 0;
        this.items = 0;
        this.dataSize = 0;
    }

    public int size() {
        return items;
    }

    public boolean isEmpty() {
        return items == 0;
    }

    public boolean containsKey(Object o) {
        return get(o) != null;
    }

    public boolean containsValue(Object o) {
        Lock lock = rwl.readLock();
        lock.lock();
        if(length > 0){
            int offset = 0;
            byte[] value = (byte[])o;
            while(offset < length){
                short keySize = utils.byteToShort(data, offset);
                short valueSize = utils.byteToShort(data, offset + KEY_SIZE_LEN);
                if(valueSize == value.length){
                    if(areEqual(value, data, offset + SIZE_LEN + CRC_LEN)){
                        return true;
                    }
                }
                offset = offset + keySize + valueSize + SIZE_LEN + CRC_LEN;
            }
        }
        lock.unlock();
        return false;
    }

    public byte[] remove(Object o) {
        byte[] value = null;
        Lock lock = rwl.writeLock();
        lock.lock();
        if(length > 0){
            int offset = 0;
            byte[] keyBytes = ((String)o).getBytes();
            short keyCrc = utils.crc16(keyBytes);
            while((offset + SIZE_LEN + CRC_LEN) < (length)){
                short keySize = utils.byteToShort(data, offset);
                short valueSize = utils.byteToShort(data, offset + KEY_SIZE_LEN);
                if(keySize == keyBytes.length){
                    short crc = utils.byteToShort(data, offset + SIZE_LEN);
                    if(keyCrc == crc && areEqual(keyBytes, data, offset + SIZE_LEN + CRC_LEN)){
                        value = new byte[valueSize];
                        System.arraycopy(data, offset + SIZE_LEN + CRC_LEN, value, 0, valueSize);
                        for(int i = 0; i < (valueSize+keySize+CRC_LEN); i++){
                            data[offset + SIZE_LEN + i] = 0;
                        }
                        items--;
                        dataSize = dataSize - keySize - valueSize - SIZE_LEN - CRC_LEN;
                    }
                }
                offset = offset + keySize + valueSize + SIZE_LEN + CRC_LEN;
            }
        }
        lock.unlock();
        return value;
    }

    private boolean areEqual(byte[] source, byte[] target, int offset) {
        int i = 0;
        for (byte b : source) {
            if(b != target[offset + i]){
                return false;
            }
            i++;
        }
        return true;
    }

    public byte[] put(String key, byte[] bytes) {
        byte[] value = null;
        Lock lock = rwl.writeLock();
        lock.lock();
        byte[] currentValue = _get(key);
        if(currentValue == null){
            byte[] keyBytes = key.getBytes();
            byte[] crcBytes = utils.shortToBytes(utils.crc16(keyBytes));
            if((bytes.length + keyBytes.length + CRC_LEN + SIZE_LEN) > (data.length - length)){
                int increase = bytes.length + keyBytes.length + SIZE_LEN + CRC_LEN - (data.length - length);
                byte[] newData = new byte[data.length + increase];
                System.arraycopy(data, 0, newData, 0, data.length);
                data = newData;
            }
            byte[] keyLenBytes = utils.shortToBytes(key.getBytes().length);
            byte[] valueLenBytes = utils.shortToBytes(bytes.length);
            System.arraycopy(keyLenBytes, 0, data, length, keyLenBytes.length);
            System.arraycopy(valueLenBytes, 0, data, length + KEY_SIZE_LEN, valueLenBytes.length);
            System.arraycopy(crcBytes, 0, data, length + SIZE_LEN, CRC_LEN);
            System.arraycopy(keyBytes, 0, data, length + SIZE_LEN + CRC_LEN, keyBytes.length);
            System.arraycopy(bytes, 0, data, length + SIZE_LEN + CRC_LEN + keyBytes.length, bytes.length);
            length += keyBytes.length + bytes.length + SIZE_LEN + CRC_LEN;
            items++;
            dataSize += keyBytes.length + bytes.length + SIZE_LEN + CRC_LEN;
            return null;
        }else{
            remove(key);
            value = put(key, bytes);
        }
        lock.unlock();
        return value;
    }

    public byte[] get(Object o) {
        Lock lock = rwl.readLock();
        lock.lock();
        byte[] value = _get(o);
        lock.unlock();
        return value;
    }
    public byte[] _get(Object o) {
        if(length > 0){
            int offset = 0;
            byte[] keyBytes = ((String)o).getBytes();
            short keyCrc = utils.crc16(keyBytes);
            while((offset + SIZE_LEN + CRC_LEN) < length){
                short keySize = utils.byteToShort(data, offset);
                short valueSize = utils.byteToShort(data, offset + KEY_SIZE_LEN);
                if(keySize < 0 || valueSize < 0){
                    System.out.println("Error");
                }
                if(keySize == keyBytes.length){
                    short crc = utils.byteToShort(data, offset + SIZE_LEN);
                    if(crc == keyCrc && areEqual(keyBytes, data, offset + SIZE_LEN + CRC_LEN)){
                        byte[] value = new byte[valueSize];
                        System.arraycopy(data, offset + SIZE_LEN + CRC_LEN + keySize , value, 0, valueSize);
                        return value;
                    }
                }
                offset = offset + keySize + valueSize + SIZE_LEN + CRC_LEN;
            }
        }
        return null;
    }

    public void putAll(Map<? extends String, ? extends byte[]> map) {
        for (String key : map.keySet()) {
            put(key, map.get(key));
        }
    }

    public void clear() {
        Lock lock = rwl.writeLock();
        lock.lock();
        init();
        lock.unlock();
    }

    public Set<String> keySet() {
        Set<String> keys = new HashSet<String>();
        Lock lock = rwl.readLock();
        lock.lock();
        if(length > 0){
            int offset = 0;
            while(offset < length){
                short keySize = utils.byteToShort(data, offset);
                short valueSize = utils.byteToShort(data, offset + KEY_SIZE_LEN);
                String key = new String(data, offset + SIZE_LEN + CRC_LEN, keySize);
                keys.add(key);
                offset = offset + keySize + valueSize + SIZE_LEN + CRC_LEN;
            }
        }
        lock.unlock();
        return keys;
    }

    public Collection<byte[]> values() {
        Lock lock = rwl.readLock();
        lock.lock();
        List<byte[]> values = new ArrayList<byte[]>();
        if(length > 0){
            int offset = 0;
            while(offset < length){
                short keySize = utils.byteToShort(data, offset);
                short valueSize = utils.byteToShort(data, offset + KEY_SIZE_LEN);
                byte[] value = new byte[valueSize];
                System.arraycopy(data, offset + SIZE_LEN + CRC_LEN + keySize, value, 0, valueSize);
                values.add(value);
                offset = offset + keySize + valueSize + SIZE_LEN + CRC_LEN;
            }
        }
        lock.unlock();
        return values;
    }

    public Set<Entry<String, byte[]>> entrySet() {
        Lock lock = rwl.readLock();
        lock.lock();
        Set<Entry<String, byte[]>> entries = new HashSet<Entry<String, byte[]>>();
        if(length > 0){
            int offset = 0;
            while(offset < length){
                short keySize = utils.byteToShort(data, offset);
                short valueSize = utils.byteToShort(data, offset + KEY_SIZE_LEN);
                final String key = new String(data, offset + SIZE_LEN + CRC_LEN, keySize);
                final byte[] value = new byte[valueSize];
                System.arraycopy(data, offset + SIZE_LEN + CRC_LEN + keySize, value, 0, valueSize);
                offset = offset + keySize + valueSize + SIZE_LEN + CRC_LEN;
                entries.add(new Entry(){

                    public Object getKey() {
                        return key;
                    }

                    public Object getValue() {
                        return value;
                    }

                    public Object setValue(Object o) {
                        throw new UnsupportedOperationException("setValue not supported");
                    }
                });
            }
        }
        lock.unlock();
        return entries;
    }
}
