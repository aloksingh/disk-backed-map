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

package com.alok.diskmap.perf;

import com.alok.diskmap.Configuration;
import com.alok.diskmap.DiskBackedMap;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Driver {
    public static void main(String[] args) throws Exception {
        Config cfg = new Config(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]), args[4]);
        if(cfg.getName().startsWith("DiskBackedMap-Reader-Threads-1")){
            Map<String, String> map = new DiskBackedMap<String, String>(new Configuration().setDataDir(new File(args[5])).setUseNonBlockingReader(true));
            IncrementingKeyValueGen generator = new IncrementingKeyValueGen();
//            populateData(map, cfg.getItemCount(), generator);
            StatsCollector collector = new StatsCollector(cfg);
            collector.writeHeader();
            PerfTask task = new Reader(map, cfg, collector, generator);
            task.run();
            collector.close();
        }else  {

                Map<String, String> map = new DiskBackedMap<String, String>(new Configuration().setDataDir(new File(args[5])).setUseNonBlockingReader(true));
                IncrementingKeyValueGen generator = new IncrementingKeyValueGen();
                populateData(map, cfg.getItemCount(), generator);
                List<Thread> threads = new ArrayList<Thread>();
                StatsCollector collector = new StatsCollector(cfg);
                collector.writeHeader();
                int THREAD_COUNT = 2;
                if(cfg.getName().startsWith("DiskBackedMap-Reader-Threads-2")){
                    THREAD_COUNT = 2;
                }
                if(cfg.getName().startsWith("DiskBackedMap-Reader-Threads-4")){
                    THREAD_COUNT = 4;
                }
                if(cfg.getName().startsWith("DiskBackedMap-Reader-Threads-6")){
                    THREAD_COUNT = 6;
                }
                for(int i = 0; i < THREAD_COUNT; i++){
                    Thread t = new Thread(new Reader(map, cfg, collector, generator));
                    threads.add(t);
                    t.start();
                }
                for (Thread thread : threads) {
                    try {
                        thread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                collector.close();
                ((DiskBackedMap) map).close();
            }
    }

    private static void populateData(Map map, int itemCount, KeyValueGen generator) {
        long time = System.currentTimeMillis();
        for(int i = 0; i < itemCount; i++){
            map.put(generator.nextKey(), generator.nextValue());
            if(i % (itemCount/100) == 0){
                time = System.currentTimeMillis() - time;
                System.out.println(String.format("Generated [%d] of [%d] in [%d] ms", i, itemCount, time));
                time = System.currentTimeMillis();
            }
        }
    }
    public static class IncrementingKeyValueGen implements KeyValueGen{
        private int currentKey = 0;

        public IncrementingKeyValueGen(int cKey){
            this.currentKey = cKey;
        }

        public IncrementingKeyValueGen(){
            this.currentKey = 0;
        }

        @Override
        public Serializable nextKey() {
            return new Integer(currentKey++);
        }

        @Override
        public Serializable nextValue() {
            return String.format("Value[ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-%d",currentKey);
        }

        @Override
        public Serializable existingKey() {
            return new Integer((int) (Math.random() * currentKey));
        }
    }
}
