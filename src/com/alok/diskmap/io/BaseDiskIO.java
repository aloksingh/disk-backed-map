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
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class BaseDiskIO implements DiskIO {
    private static final Logger logger = Logger.getLogger(BlockingDiskIO.class.getName());
    protected File file;
    private RandomAccessFile writer;
    private RandomAccessFile reader;
    private long lastFlush;
    protected Configuration config;
    private static final boolean DEBUG = false;

    public BaseDiskIO(Configuration config, File f){
        try {
            this.config = config;
            if(f == null){
                this.file = new File(config.getDataFileName("dat"));
            }
            if (!this.file.exists()) {
                boolean created = this.file.createNewFile();
                if(!created){
                    throw new RuntimeException(String.format("Unable to create file: %s", this.file.getAbsolutePath()));
                }
            }
            createFileHandlers();
        }catch(Exception e){
            throw newRuntimeException(e);
        }
    }

    protected void createFileHandlers() {
        try{
            this.setReader(new RandomAccessFile(this.file, "r"));
            this.setWriter(new RandomAccessFile(this.file, "rw"));
            this.getWriter().seek(reader().length());
        }catch(Exception e){
            close(getReader());
            close(getWriter());
        }
    }

    @Override
    public Iterator<Record> iterator() {
        final RandomAccessFile rc;
        try {
            rc = new RandomAccessFile(file, "r");
            if (rc.length() > 0) {
                rc.seek(0);
            }
        } catch (Exception e) {
            throw newRuntimeException(e);
        }

        return new Iterator<Record>(){
            @Override
            public boolean hasNext() {
                try {
                    if(rc.getFilePointer() < rc.length()){
                        return true;
                    }
                    close(rc);
                    return false;
                } catch (IOException e) {
                    close(rc);
                    throw newRuntimeException(e);
                }
            }

            @Override
            public Record next() {
                try {
                    Record r = new Record();
                    r.read(rc);
                    return r;
                } catch (IOException e) {
                    close(rc);
                    throw newRuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove is not supported");
            }
        };
    }

    private void close(RandomAccessFile rc) {
        try {rc.close();} catch (IOException ioe) {logger.log(Level.SEVERE, ioe.getMessage(), ioe);}
    }

    protected RuntimeException newRuntimeException(Exception e) throws RuntimeException{
        logger.log(Level.SEVERE, e.getMessage(), e);
        throw new RuntimeException(e);
    }

    @Override
    public void close() {
        close(this.getWriter());
        close(this.getReader());
        this.file = null;
    }

    public abstract Record lookup(long location);

    public Record doLookup(long location) {
        try {
            Record r = new Record();
            RandomAccessFile reader = reader();
            synchronized (reader){
//                reader.seek(location);
                r.read(reader.getChannel(), location);
            }
            return r;
        } catch (IOException e) {
            logger.log(Level.SEVERE, String.format("lookup(%d) failed", location));
            throw newRuntimeException(e);
        }
    }

    @Override
    public abstract long write(Record r);

    protected long doWrite(Record r, RandomAccessFile writer) throws IOException{
        long location = writer.getFilePointer();
        Record newRecord = new Record(r, location);
        newRecord.write(writer);
        return location;
    }

    protected void doFlush() {
        try {
                writer().getChannel().force(false);
                this.lastFlush = System.currentTimeMillis();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
    }

    protected final RandomAccessFile writer() {
        return getWriter();
    }

    protected final RandomAccessFile reader() {
        return getReader();
    }

    protected Configuration getConfig() {
        return config;
    }

    @Override
    public abstract void vacuum(RecordFilter filter) throws Exception;

    @Override
    public void clear() {
        closeFileHandlers();
        boolean b = new File(config.getDataFileName("dat")).delete();
        if(b){
            try {
                boolean created = new File(config.getDataFileName("dat")).createNewFile();
                if(created){
                    createFileHandlers();
                    return;
                }
            } catch (IOException e) {
                throw new RuntimeException("Unable to clear file: " + config.getDataFileName("dat"), e);
            }
        }
        throw new RuntimeException("Unable to clear file: " + config.getDataFileName("dat"));
    }

    public void doVacuum(RecordFilter filter) throws Exception {
        doFlush();
        closeFileHandlers();
        File newFile = new File(config.getDataFileName("tmp"));
        RandomAccessFile newWriter = new RandomAccessFile(newFile, "rw");
        for(Record r : this){
            if(filter.accept(r)){
                long location = doWrite(r, newWriter);
                filter.update(r, location);
            }
        }
        newWriter.close();
        if(this.file.renameTo(new File(config.getDataFileName("bak")))){
            if(newFile.renameTo(new File(config.getDataFileName("dat")))){
                this.file = new File(config.getDataFileName("dat"));
                createFileHandlers();
                new File(config.getDataFileName("bak")).delete();
            }else{
                throw new RuntimeException("Unable to vacuum the data file.");
            }
        }else{
            throw new RuntimeException("Unable to vacuum the data file.");
        }
    }

    @Override
    public long size(){
        try {
            return writer().getFilePointer();
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return 0;
    }

    private void closeFileHandlers() {
        close(getReader());
        close(getWriter());
    }

    @Override
    public abstract void update(Record...records);

    @Override
    public abstract void update(Record record);

    public void doUpdate(Record record) throws IOException {
        long currentLocation = writer().getFilePointer();
        writer().seek(record.getLocation());
        record.write(writer());
        writer().seek(currentLocation);
    }


    public void doUpdate(Record...records) throws IOException {
        long currentLocation = writer().getFilePointer();
        Arrays.sort(records);
        for (Record record : records) {
            writer().seek(record.getLocation());
            record.write(writer());
        }
        writer().seek(currentLocation);
    }

    private RandomAccessFile getWriter() {
        return writer;
    }

    public void setWriter(RandomAccessFile writer) {
        this.writer = writer;
    }

    private RandomAccessFile getReader() {
        return reader;
    }

    public void setReader(RandomAccessFile reader) {
        this.reader = reader;
    }

    protected long getLastFlush() {
        return lastFlush;
    }

    public boolean isDebug(){
        return DEBUG;
    }
}
