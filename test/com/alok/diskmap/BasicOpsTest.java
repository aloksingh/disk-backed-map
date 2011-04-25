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

import junit.framework.TestCase;

import java.io.File;
import java.io.Serializable;
import java.util.Map;

public class BasicOpsTest extends TestCase {
    private static final String TMP_DIR = "/tmp/tests";
//    private static final String TMP_DIR = "/home/alok/sw_dev/tmp/tests";

    public void setUp(){
        File f = new File(TMP_DIR);
        if(!f.exists()){
            f.mkdirs();
        }
    }
    public void testSimplePut(){
        String keyS = "test";
        String value = "valueString";
        String value2 = "valueString2";
        Map<Serializable, Serializable> map = getNBMap();
        map.put(keyS, value);
        assertEquals(value, map.get(keyS));
        map.put(keyS, value2);
        assertEquals(value2, map.get(keyS));
    }

    public void testDelete() throws Exception{
        int count = 10000;
        DiskBackedMap<Serializable, Serializable> map = getNBMap();
        for(int i = 0; i < count; i++){
            map.put("Key" + i, "Value" + i);
        }
        long originalSize = map.sizeOnDisk();
        for(int i = 0; i < count; i++){
            if( i % 5 == 0){
                map.remove("Key" + i);
            }
        }
        assertEquals(originalSize, map.sizeOnDisk());
        map.gc();
        assertTrue(originalSize > map.sizeOnDisk());
        DiskBackedMap<Serializable, Serializable> map2 = getMap();
        for(int i = 0; i < count; i++){
            if( i % 5 != 0){
                assertEquals("Value" + i, map2.get("Key" + i));
            }
        }
    }

    public void testHashCollisions(){
        StringWithDuplicateHash str1 = new StringWithDuplicateHash("Foo", 1);
        StringWithDuplicateHash str2 = new StringWithDuplicateHash("Bar", 1);
        StringWithDuplicateHash str3 = new StringWithDuplicateHash("FooBar", 1);
        Map<Serializable, Serializable> map = getNBMap();
        map.put(str1, str1.getValue());
        map.put(str2, str2.getValue());
        assertEquals(str1.getValue(), map.get(str1));
        assertEquals(str2.getValue(), map.get(str2));
        assertNull(map.get(str3));
        assertFalse(map.containsKey(str3));
    }

    public static class StringWithDuplicateHash implements Serializable{
        private String value;
        private int hash;

        public StringWithDuplicateHash(){
        }
        public StringWithDuplicateHash(String value, int hash){
            this.value = value;
            this.hash = hash;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public int getHash() {
            return hash;
        }

        public void setHash(int hash) {
            this.hash = hash;
        }

        @Override
        public int hashCode(){
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StringWithDuplicateHash that = (StringWithDuplicateHash) o;

            if (value != null ? !value.equals(that.value) : that.value != null) return false;

            return true;
        }
    }

    private DiskBackedMap<Serializable, Serializable> getMap() {
        return new DiskBackedMap<Serializable, Serializable>(new Configuration().setDataDir(new File(TMP_DIR)));
    }

    private DiskBackedMap<Serializable, Serializable> getNBMap() {
        return new DiskBackedMap<Serializable, Serializable>(new Configuration().setDataDir(new File(TMP_DIR)).setUseNonBlockingReader(true));
    }

}
