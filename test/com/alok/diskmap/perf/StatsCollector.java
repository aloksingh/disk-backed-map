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

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class StatsCollector {
    private Config config;
    private Writer writer;

    public StatsCollector(Config config) {
        this.config = config;
        try {
            this.writer = new FileWriter(this.config.getStatsDir());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public void writeHeader(){
        synchronized (writer){
            try {
                writer.write(String.format("Name [%s], Total Items[%d], Total Operations[%d]", config.getName(), config.getItemCount(), config.getRunCount()));
                writer.write("\n");
                writer.write("Items,Time");
                writer.write("\n");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void close(){
        synchronized (writer){
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public void update(int step, long time) {
        synchronized (writer){
            try {
                writer.write(String.format("%d,%d\n", step, time));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
