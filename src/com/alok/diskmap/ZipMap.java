package com.alok.diskmap;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by IntelliJ IDEA.
 * User: aloksingh
 * Date: 11/1/11
 * Time: 11:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class ZipMap implements Map<String, byte[]>{
    private byte[] data;
    private int length;
    private int dataSize;
    private int items;
    private ConversionUtils utils = ConversionUtils.instance;
    ReadWriteLock rwl = new ReentrantReadWriteLock();

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
                int keySize = utils.byteToInt(data, offset);
                int valueSize = utils.byteToInt(data, offset + 4);
                if(valueSize == value.length){
                    if(areEqual(value, data, offset+8)){
                        return true;
                    }
                }
                offset = offset + keySize + valueSize + 8;
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
            while(offset < length){
                int keySize = utils.byteToInt(data, offset);
                int valueSize = utils.byteToInt(data, offset + 4);
                if(keySize == keyBytes.length){
                    if(areEqual(keyBytes, data, offset+8)){
                        value = new byte[valueSize];
                        System.arraycopy(data, offset+8, value, 0, valueSize);
                        for(int i = 0; i < (valueSize+keySize); i++){
                            data[offset + 8 + i] = -1;
                        }
                        items--;
                        dataSize = dataSize - keySize - valueSize -8;
                    }
                }
                offset = offset + keySize + valueSize + 8;
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
            if((bytes.length + keyBytes.length) > (data.length - length)){
                byte[] newData = new byte[data.length + bytes.length + keyBytes.length + 8];
                data = newData;
            }
            byte[] keyLenBytes = utils.intToBytes(key.getBytes().length);
            byte[] valueLenBytes = utils.intToBytes(bytes.length);
            System.arraycopy(keyLenBytes, 0, data, length, keyLenBytes.length);
            System.arraycopy(valueLenBytes, 0, data, length+4, valueLenBytes.length);
            System.arraycopy(keyBytes, 0, data, length+8, keyBytes.length);
            System.arraycopy(bytes, 0, data, length+8+keyBytes.length, bytes.length);
            length += keyBytes.length + bytes.length+8;
            items++;
            dataSize += keyBytes.length + bytes.length + 8;
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
            while(offset < length){
                int keySize = utils.byteToInt(data, offset);
                int valueSize = utils.byteToInt(data, offset + 4);
                if(keySize == keyBytes.length){
                    if(areEqual(keyBytes, data, offset+8)){
                        byte[] value = new byte[valueSize];
                        System.arraycopy(data, offset+8+keySize, value, 0, valueSize);
                        return value;
                    }
                }
                offset = offset + keySize + valueSize + 8;
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
                int keySize = utils.byteToInt(data, offset);
                int valueSize = utils.byteToInt(data, offset + 4);
                String key = new String(data, offset + 8, keySize);
                keys.add(key);
                offset = offset + keySize + valueSize + 8;
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
                int keySize = utils.byteToInt(data, offset);
                int valueSize = utils.byteToInt(data, offset + 4);
                byte[] value = new byte[valueSize];
                System.arraycopy(data, offset + 8 + keySize, value, 0, valueSize);
                values.add(value);
                offset = offset + keySize + valueSize + 8;
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
                int keySize = utils.byteToInt(data, offset);
                int valueSize = utils.byteToInt(data, offset + 4);
                final String key = new String(data, offset + 8, keySize);
                final byte[] value = new byte[valueSize];
                System.arraycopy(data, offset + 8 + keySize, value, 0, valueSize);
                offset = offset + keySize + valueSize + 8;
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
