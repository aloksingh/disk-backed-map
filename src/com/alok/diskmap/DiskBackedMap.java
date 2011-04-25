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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DiskBackedMap<K extends Serializable, V extends Serializable> implements Map<K, V>, Closeable {
    private Logger log = Logger.getLogger(DiskBackedMap.class.getName());
    private Store<K, V> store;

    public DiskBackedMap(String dataDir) {
        this.store = new Store<K, V>(new Configuration().setDataDir(new File(dataDir)));
    }

    public DiskBackedMap(Configuration config) {
        this.store = new Store<K, V>(config);
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return store.get((K) key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return null;
    }

    @Override
    @SuppressWarnings("element-type-mismatch")
    public V get(Object key) {
        return store.get((K) key);
    }

    @Override
    public boolean isEmpty() {
        return store.size() == 0;
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V put(K key, V value) {
        return store.save(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (K key : m.keySet()) {
            put(key, m.get(key));
        }
    }

    @Override
    public V remove(Object key) {
        V value = store.get((K) key);
        store.remove((K) key);
        return value;
    }

    @Override
    public int size() {
        return store.size();
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    public long sizeOnDisk(){
        return store.sizeOnDisk();
    }

    public void close() throws IOException {
        store.close();
    }

    public void gc() throws Exception {
        store.vacuum();
    }

    @Override
    public void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    public class Store<K extends Serializable, V extends Serializable> implements Closeable {
        private List<Page<K, V>> pages;
        private int magicNumber = 13;

        public Store(Configuration cfg) {
            init(cfg);
        }

        private void init(Configuration cfg) {
            pages = new ArrayList<Page<K, V>>(magicNumber);
            for (int i = 0; i < magicNumber; i++) {
                Configuration config = new Configuration(cfg);
                config.setNumber(i);
                pages.add(new Page<K, V>(config));
            }
        }

        public V save(K key, V value) {
            Page<K, V> kvPage = findPage(key);
            return kvPage.save(key, value);
        }

        public V get(K key) {
            return  findPage(key).load(key);
        }

        private Page<K, V> findPage(K key) {
            int idx = key.hashCode() % magicNumber;
            return pages.get(Math.abs(idx));
        }

        private void remove(K  key) {
            findPage(key).remove(key);
        }

        private int size() {
            int size = 0;
            for (Page<K, V> page : pages) {
                size = page.keyCount();
            }
            return size;
        }

        public synchronized void close() {
            for (Page page : pages) {
                page.close();
            }
        }

        public void vacuum() throws Exception {
            log.log(Level.INFO, "Starting gc process");
            long time = 0;
            for (Page<K, V> page : pages) {
                long pTime = System.currentTimeMillis();
                log.log(Level.INFO, "Started Vacuuming page:" + page.toString());
                page.vacuum();
                pTime = System.currentTimeMillis() - pTime;
                log.log(Level.INFO, "Completed Vacuuming page in :" + pTime + " ms");
                time += pTime;
            }
            log.log(Level.INFO, "Vacuum Complete:" + time + " ms");
        }

        public long sizeOnDisk() {
            long size = 0;
            for (Page<K, V> page : pages) {
                size = page.size();
            }
            return size;
        }

        public synchronized  void clear() {
            for (Page<K, V> page : pages) {
                page.clear();
            }
        }
    }
}