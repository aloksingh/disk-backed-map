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

import java.io.Serializable;
import java.util.Map;

public class Reader extends PerfTask{

    public Reader(Map map, Config config, StatsCollector statsCollector, KeyValueGen generator){
        super(map, statsCollector, config, generator);
    }

    @Override
    protected Object execute(StatsCollector statsCollector, int step) {
        long time = 0;
        long hash = 0;
        long missCount = 0;
        for(int i = 0; i < step; i++){
            long stepTime = System.nanoTime();
            Serializable key = getGenerator().existingKey();
            Object value = getMap().get(key);
            stepTime = System.nanoTime() - stepTime;
            time+= stepTime;
            hash += (value != null ? value.hashCode() : 0);
            if(value == null || value.toString().length() < 36){
                missCount++;
            }
        }
        statsCollector.update(step, time);
        System.out.println(String.format("Missed %d out of %d", missCount, step));
        return hash;
    }


}
