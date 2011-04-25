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

package com.alok.diskmap.io;

import com.alok.diskmap.Configuration;
import com.alok.diskmap.Record;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BlockingDiskIO extends BaseDiskIO {

    private static final Logger logger = Logger.getLogger(BlockingDiskIO.class.getName());

    public BlockingDiskIO(Configuration config, String file){
        super(config, new File(file));
    }
    public BlockingDiskIO(Configuration config){
        super(config, null);
    }
    public BlockingDiskIO(Configuration config, File f){
        super(config, f);
    }

    @Override
    public Record lookup(long location) {
        return doLookup(location);
    }

    @Override
    public long write(Record r) {
        try {
            return doWrite(r, writer());
        } catch (IOException e) {
            throw newRuntimeException(e);
        }
    }


    private void flush() {
        if(System.currentTimeMillis() - this.getLastFlush() >= getConfig().getFlushInterval()){
            doFlush();
        }
    }

    @Override
    public void vacuum(RecordFilter filter) throws Exception {
        doVacuum(filter);
    }

    @Override
    public void update(Record record) {
       try {
           doUpdate(record);
           flush();
       } catch (IOException e) {
           logger.log(Level.SEVERE, e.getMessage(), e);
       }
    }

    @Override
    public void update(Record...records) {
        try {
            doUpdate(records);
            flush();
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }


}
