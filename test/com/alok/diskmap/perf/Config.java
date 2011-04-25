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

import java.io.File;

public class Config {
    private int itemCount;
    private int runCount;
    private int step;
    private String statsDir;
    private String name;

    public Config(String name, Integer itemCount, Integer runCount, Integer step, String statsDir) {
        this.name = name;
        this.itemCount = itemCount;
        this.runCount = runCount;
        this.step = step;
        this.statsDir = statsDir;
    }

    public int getItemCount() {
        return itemCount;
    }

    public int getRunCount() {
        return runCount;
    }

    public int getStep() {
        return step;
    }

    public String getStatsDir() {
        return statsDir + File.separator + getName().replace(' ', '_') + ".txt";
    }

    public String getName() {
        return name;
    }
}
