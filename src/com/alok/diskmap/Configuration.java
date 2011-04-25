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

import java.io.File;

public class Configuration {
    private int flushInterval;
    private File dir;
    private int number;
    private int readerPoolSize = 3;
    private boolean useNonBlockingReader = true;

    public Configuration() {
    }

    public Configuration(Configuration cfg) {
        this.flushInterval = cfg.getFlushInterval();
        this.dir = new File(cfg.getDataDir().getAbsolutePath());
        this.number = cfg.getNumber();
        this.readerPoolSize = cfg.getReaderPoolSize();
        this.useNonBlockingReader = cfg.getUseNonBlockingReader();
    }

    public Configuration setFlushInterval(int interval) {
        this.flushInterval = interval;
        return this;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public Configuration setDataDir(File dir) {
        this.dir = dir;
        return this;
    }

    public File getDataDir() {
        return dir;
    }

    public Configuration setNumber(int number) {
        this.number = number;
        return this;
    }

    public int getNumber() {
        return number;
    }

    public String getDataFileName(String extension){
        return getDataDir().getAbsolutePath() + File.separator + getNumber() + "." + extension;
    }

    public int getReaderPoolSize() {
        return readerPoolSize;
    }

    public boolean getUseNonBlockingReader() {
        return useNonBlockingReader;
    }

    public Configuration setUseNonBlockingReader(boolean useNonBlockingReader) {
        this.useNonBlockingReader = useNonBlockingReader;
        return this;
    }
}
