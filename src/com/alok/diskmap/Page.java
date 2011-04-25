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

import com.alok.diskmap.io.BlockingDiskIO;
import com.alok.diskmap.io.DiskIO;
import com.alok.diskmap.io.NonBlockingDiskIO;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Page<K extends Serializable, V extends Serializable> implements Closeable {
    private static final Logger logger = Logger.getLogger(Page.class.getName());
    private static final boolean DEBUG = false;
    private RBTree layout;
    private final Configuration cfg;
    private DiskIO io;
    private ConversionUtils cUtils = ConversionUtils.instance;

    private ReadWriteLock rwl = new ReentrantReadWriteLock();


    public Page(File dir, int number) {
        this(new Configuration().setFlushInterval(1000).setDataDir(dir).setNumber(number));
    }

    public Page(Configuration cfg) {
        this.cfg = cfg;
        layout = new RBTree();
        this.io = this.cfg.getUseNonBlockingReader() ? new NonBlockingDiskIO(cfg) : new BlockingDiskIO(cfg); 
        loadData(io);
    }

    public V load(K key) {
        this.rwl.readLock().lock();
        try{
            Record record = loadRecord(key);
            return record == null ? null : cUtils.<V>deserialize(record.getValue());
        }catch(Exception e){
            logger.log(Level.SEVERE, String.format("%s load([%s]) failed", cfg.getDataFileName("dat"), String.valueOf(key)), e);
            throw new RuntimeException(e);
        }finally {
            this.rwl.readLock().unlock();
        }
    }

    private Record loadRecord(Serializable key) {
        long[] locations = layout.lookup(key.hashCode());
        try {
            if (locations != null) {
                if (locations.length == 1) {
                    Record record = io.lookup(locations[0]);
                    return cUtils.deserialize(record.getKey()).equals(key) ? record : null;
                } else {
                    for (long location : locations) {
                        Record r = io.lookup(location);
                        if (key.equals(cUtils.deserialize(r.getKey()))) {
                            return r;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, String.format("%s loadRecord([%s]) failed. Locations [%s]", this.cfg.getDataFileName(".dat"), String.valueOf(key), Arrays.toString(locations)), e);
            throw new RuntimeException(e);
        }
        return null;
    }

    public V save(K key, V value) {
        rwl.writeLock().lock();
        log(Level.INFO, "[%s] save([%s], [%s]) started", cfg.getDataFileName("dat"), key, value);
        try {
            byte[] kBuffer = cUtils.serialize(key);
            byte[] vBuffer = cUtils.serialize(value);
            Record r = new Record(kBuffer, vBuffer, Record.ACTIVE, key.hashCode(), -1);
            long location = io.write(r);
            //Check to see if a old record exists
            Record oldRecord = loadRecord(key);
            if(oldRecord != null){
                layout.delete(oldRecord.getHash(), oldRecord.getLocation());
            }
            updateLayout(r, location);
            r.setLocation(location);
            if(oldRecord != null){
                oldRecord.setFlag(Record.DELETED);
                io.update(oldRecord, r);
            }else{
                io.update(r);
            }
            log(Level.INFO, "[%s] save([%s], [%s]) complete. Record[%s]", cfg.getDataFileName("dat"), key, value, r);
            return (V) cUtils.deserialize(vBuffer);
        } catch (Exception e) {
            logger.log(Level.SEVERE, String.format("[%s] save([%s], [%s]) failed", cfg.getDataFileName("dat"), String.valueOf(key), String.valueOf(value)), e);
            throw new RuntimeException(e);
        }finally {
            rwl.writeLock().unlock();
        }
    }

    private void log(Level level, String msg, Object...args) {
        if(logger.isLoggable(level)){
           String[] sArgs = new String[args.length];
            for (int i = 0; i < args.length; i++) {
                sArgs[i] = String.valueOf(args[i]);
            }
            if(level.equals(Level.INFO)){
                if(DEBUG){
                    logger.log(level, String.format(msg, args));
                }
            }else{
                logger.log(level, msg);
            }
        }
    }

    public void remove(K key) {
        rwl.writeLock().lock();
        try{
        Record oldRecord = loadRecord(key);
        if(oldRecord != null){
            layout.delete(oldRecord.getHash(), oldRecord.getLocation());
            oldRecord.setFlag(Record.DELETED);
            io.update(oldRecord);
        }
        }catch(Exception e){
            logger.log(Level.SEVERE, String.format("[%s] remove([%s]) failed", cfg.getDataFileName("dat"), String.valueOf(key)), e);
        }finally {
            rwl.writeLock().unlock();
        }
    }


    private List<Record> lookup(long[] locations) throws IOException {
        List<Record> records = new ArrayList<Record>(locations.length);
        for (Long location : locations) {
            Record r = io.lookup(location);
            records.add(r);
        }
        return records;
    }

    private void updateLayout(Record r, long location) {
        layout.insert(r.getHash(), location);
    }

    public Iterator<Map.Entry<K, V>> iterator() {
        return new Iterator<Map.Entry<K, V>>() {
            private Node current = layout.root;
            private List<Record> records = new ArrayList<Record>();

            public boolean hasNext() {
                loadNext();
                return records.size() > 0;
            }

            public Entry<K, V> next() {
                try {
                    final K key = cUtils.<K>deserialize(records.get(0).getKey());
                    final V value = cUtils.<V>deserialize(records.get(0).getValue());
                    records.remove(0);
                    return new Entry<K, V>() {
                        public K getKey() {
                            return key;
                        }

                        public V getValue() {
                            return value;
                        }

                        public V setValue(V value) {
                            throw new UnsupportedOperationException("Not supported yet.");
                        }
                    };
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            public void remove() {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            private void loadNext() {
                try {
                    if (records.size() == 0 && current != null) {
                        if (current.getValues() == null) {
                            records.add(io.lookup(current.getValue()));
                        } else {
                            records.addAll(lookup(current.getValues()));
                        }
                        if (current.left != null) {
                            current = current.left;
                        } else if (current.right != null) {
                            current = current.right;
                        } else {
                            current = null;
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        };
    }

    private void loadData(DiskIO io) {
        rwl.writeLock().lock();
        try{
            log(Level.INFO, "%s loadData started", cfg.getDataFileName("dat"));
            long time = System.currentTimeMillis();
            int count = 0;
            for (Record r : io) {
                if (r.getFlag() == Record.ACTIVE && r.getLocation() != -1) {
                    layout.insert(r.getHash(), r.getLocation());
                }
                count++;
            }
            log(Level.INFO, "%s loadData loadData complete. Items: %s in ms: %s", count, (System.currentTimeMillis() - time),cfg.getDataFileName("dat"));
        }catch(Exception e){
            log(Level.SEVERE, String.format("%s loadData failed", cfg.getDataFileName("dat")));
        }finally {
            rwl.writeLock().unlock();
        }
    }

    public void vacuum() throws Exception {
        rwl.writeLock().lock();
        try{
            log(Level.INFO, "%s vaccum/gc started", cfg.getDataFileName("dat"));
            io.vacuum(new CurrentRecordFilter(layout));
            log(Level.INFO, "%s vaccum/gc complete", cfg.getDataFileName("dat"));
        }catch(Exception e){
            log(Level.SEVERE, String.format("%s vaccum/gc failed", cfg.getDataFileName("dat")));
        }finally {
            rwl.writeLock().unlock();
        }
    }


    public void close() {
        io.close();
    }
    
    public long size() {
        return this.io.size();
    }

    public int keyCount() {
        return this.layout.count();
    }

    public void clear() {
        rwl.writeLock().lock();
        try{
            log(Level.INFO, "%s clearing", cfg.getDataFileName("dat"));
            io.clear();
            this.layout = new RBTree();
            log(Level.INFO, "%s cleared", cfg.getDataFileName("dat"));
        }catch(Exception e){
            log(Level.SEVERE, String.format("%s clearing failed", cfg.getDataFileName("dat")));
        }finally {
            rwl.writeLock().unlock();
        }
    }

    private class CurrentRecordFilter implements DiskIO.RecordFilter{
        private RBTree layout;

        public CurrentRecordFilter(RBTree layout){
            this.layout = layout;
        }

        @Override
        public boolean accept(Record r) {
            long[] locations = layout.lookup(r.getHash());
            if (locations == null || locations.length == 0) {
                return false;
            }
            for (long location : locations) {
                if (location == r.getLocation()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void update(Record r, long newLocation) {
            rwl.writeLock().lock();
            try{
                log(Level.INFO, "%s update started", cfg.getDataFileName("dat"));
                layout.delete(r.getHash(), r.getLocation());
                layout.insert(r.getHash(), newLocation);
            }catch(Exception e){
                log(Level.SEVERE, String.format("%s update failed", cfg.getDataFileName("dat")));
            }finally {
                rwl.writeLock().unlock();
            }
        }
    }

    @Override
    public String toString() {
        return "Page{" +
                "data=" + cfg.getDataFileName("dat")+
                '}';
    }
}
