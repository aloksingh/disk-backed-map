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

package com.alok.diskmap.mem;

import com.alok.diskmap.Configuration;
import com.alok.diskmap.DiskBackedMap;
import com.sun.org.apache.xpath.internal.operations.Bool;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

public class MemoryUsageTest extends TestCase {
    public void testLargeArray(){
        String[] array = new String[100*1000*1000];
        for(int i = 0; i < array.length; i++){
            array[i] = UUID.randomUUID().toString();
        }
    }

    public void testHashMapMemoryUsage(){
        long startMemory = Runtime.getRuntime().totalMemory();
        Map<Integer, String> map = new HashMap<Integer, String>();
        for(int i = 1; i < Integer.MAX_VALUE; i++){
            if(i %10000 == 0){
                System.out.println("Average Used Memory:" + (Runtime.getRuntime().totalMemory())/i);
                System.out.println("entries:" + i);
            }
            map.put(new Integer(i), "Abcdefghijklmnopqrstuvwxyz" + Math.random());
        }
    }

    public void testDiskMapMemoryUsage(){
        long startMemory = Runtime.getRuntime().totalMemory();
        Configuration configuration = new Configuration();
        configuration.setDataDir(new File("/tmp/tests")).setFlushInterval(20000);
        Map<String, String> map = new DiskBackedMap<String, String>(configuration);
        long start = System.currentTimeMillis();
        long loopStart = System.currentTimeMillis();
        final int items = 100 * 1000 * 1000;
        final int loopItems = 200000;
        for(int i = 1; i < items; i++){
            if(i % loopItems == 0){
                System.out.println("Average Used Memory:" + (Runtime.getRuntime().totalMemory())/i);
                System.out.println(String.format("entries: %(,d", i));
                System.out.println(String.format("Loop time: %(,d", (System.currentTimeMillis() - loopStart)));
                System.out.println(String.format("Total time: %(,d", System.currentTimeMillis() - start));
                loopStart = System.currentTimeMillis();
            }
            String key = "App-user-" + i + "-tag-" + (i % 5);
            map.put(key, "Abcdefghijklmnopqrstuvwxyz" + Math.random());
        }
        long code = 0;

    }

    public void testConcurrentLookup(){
        final long start = System.currentTimeMillis();
        final int items = 100 * 1000 * 1000;
        final int loopItems = 200000;
        Configuration configuration = new Configuration();
        configuration.setDataDir(new File("/tmp/tests")).setFlushInterval(20000);
        final Map<String, String> map = new DiskBackedMap<String, String>(configuration);
        Callable<Boolean> reader = new Callable<Boolean>() {
            @Override
            public Boolean call() {
                long loopStart = System.currentTimeMillis();
                long code = 0;
                for(int i = 1; i < items; i++){
                    if(i % loopItems == 0){
                        System.out.println("Average Lookuptime:" + (System.currentTimeMillis() - start)/i);
                        System.out.println(String.format("entries: %(,d", i));
                        System.out.println(String.format("Loop time: %(,d", (System.currentTimeMillis() - loopStart)));
                        System.out.println(String.format("Total time: %(,d", System.currentTimeMillis() - start));
                        loopStart = System.currentTimeMillis();
                    }
                    String key = "App-user-" + i + "-tag-" + (i % 5);
                    String value = map.get(key);
                    if(value != null){
                        code += value.hashCode();
                    }else{
                        System.out.println("Missed :" + key);
                    }
                }
                return true;
            }
        };
        List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for(int i = 0; i < 10; i++){
            futures.add(executorService.submit(reader));
        }
        for (Future<Boolean> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
