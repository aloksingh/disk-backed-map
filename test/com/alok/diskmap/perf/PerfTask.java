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

import java.util.Map;

public abstract class PerfTask implements Runnable{
    private Map map;
    private Config config;
    private StatsCollector statsCollector;
    private KeyValueGen generator;

    public PerfTask(Map map, StatsCollector statsCollector, Config config, KeyValueGen generator) {
        this.map = map;
        this.statsCollector = statsCollector;
        this.config = config;
        this.generator = generator;
    }

    public void run(){
        for(int i = 0; i < config.getRunCount(); i = i + config.getStep()){
            execute(statsCollector, config.getStep());
        }
    }

    public Map getMap() {
        return map;
    }

    public Config getConfig() {
        return config;
    }

    public StatsCollector getStatsCollector() {
        return statsCollector;
    }

    public KeyValueGen getGenerator() {
        return generator;
    }

    protected abstract Object execute(StatsCollector statsCollector, int step);
}
