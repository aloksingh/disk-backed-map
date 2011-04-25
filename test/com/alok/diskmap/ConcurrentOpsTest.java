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

import junit.framework.Assert;
import junit.framework.TestCase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConcurrentOpsTest extends TestCase {
    private static final int THREAD_COUNT = 50;
    private static final String TEST_DIR = "/tmp/conc_tests";

    public void setUp(){
       File f = new File(TEST_DIR);
        if(!f.exists()){
            f.mkdirs();
        }
    }

    public void testConcurrentReadWrite() throws Exception{
        DiskBackedMap<String, String> map = new DiskBackedMap<String, String>(TEST_DIR);
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
        for(int i = 0; i < THREAD_COUNT * 10; i++){
            tasks.add(new ReaderWriter(map, 10000));
            if(i % THREAD_COUNT == 0){
                tasks.add(new Vacummer(map));
            }
        }
        executorService.invokeAll(tasks);
    }

    public class ReaderWriter implements Callable<Boolean>{
        private Map<String, String> map;
        private long count;

        public ReaderWriter(Map<String, String> map, long count){
            this.map = map;
            this.count = count;
        }

        @Override
        public Boolean call() throws Exception {
            try{
                for(int i = 0; i < count; i++){
                    String key = UUID.randomUUID().toString();
                    String value = "Abcd" + key;
                    String actualValue = null;
                    try{
                        map.put(key, value);
                        actualValue = map.get(key);
                    }catch(Exception e) {
//                        System.out.println("Put failed");
                    }
                    Assert.assertEquals(actualValue, value);
//                    if(i % 100 == 0){
//                        System.out.println("Removing key:" + key);
//                        map.remove(key);
//                    }
                }
            }catch(Exception e){
                e.printStackTrace();
            }
            return true;
        }
    }

    public class Vacummer implements Callable<Boolean>{
        private DiskBackedMap<String, String> map;

        public Vacummer (DiskBackedMap<String, String> map){
            this.map = map;
        }

        @Override
        public Boolean call() throws Exception {
            map.gc();
            return true;
        }
    }
}
