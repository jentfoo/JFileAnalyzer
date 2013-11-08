package com.jentfoo.file;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

import org.threadly.concurrent.PrioritySchedulerInterface;
import org.threadly.util.ExceptionUtils;

public class FileCrawler {
  private static final short MAX_FILES_PER_THREAD = 1000;
  private static final long MAX_SIZE_PER_THREAD = 1024L * 1024L * 1024L * 10; // 10 GB
  
  private final PrioritySchedulerInterface scheduler;
  private final List<FileListenerInterface> listeners;
  private final List<FileFilterInterface> filters;
  
  public FileCrawler(PrioritySchedulerInterface scheduler) {
    this.scheduler = scheduler;
    this.listeners = new LinkedList<FileListenerInterface>();
    this.filters = new LinkedList<FileFilterInterface>();
  }
  
  public void addListener(FileListenerInterface listener) {
    if (listener != null) {
      listeners.add(listener);
    }
  }
  
  public void addFilter(FileFilterInterface filter) {
    if (filter != null) {
      filters.add(filter);
    }
  }

  public void crawlDirectories(List<File> examineDirectories) throws IOException {
    List<Future<?>> futures = new LinkedList<Future<?>>();
    
    crawlDirectories(examineDirectories, futures);
    System.out.println("Processing " + futures.size() + " work units\n");
    
    // block till all computation has completed
    FutureUtil.blockTillAllDone(futures);
  }
  
  private void crawlDirectories(List<File> examineDirectories, 
                                List<Future<?>> futures) throws IOException {
    List<File> toInspectDirectories = new LinkedList<File>();
    long toInspectSize = 0;
    List<File> toInspectFiles = new LinkedList<File>();
    Iterator<File> it = examineDirectories.iterator();
    while (it.hasNext()) {
      File directory = it.next().getCanonicalFile();
      File[] contents = FileUtils.getFolderContents(directory);
      if (contents == null) {
        continue;
      }
      
      boolean containsDirectory = false;
      for (File f : contents) {
        boolean exclude = false;
        Iterator<FileFilterInterface> filterIt = filters.iterator();
        while (filterIt.hasNext()) {
          if (filterIt.next().shouldExclude(f)) {
            exclude = true;
            break;
          }
        }
        if (exclude) {
          continue;
        }
        
        if (f.isDirectory()) {
          containsDirectory = true;
          toInspectDirectories.add(f);
        } else {
          toInspectFiles.add(f);
          toInspectSize += f.length();
          
          if (toInspectSize >= MAX_SIZE_PER_THREAD || 
              toInspectFiles.size() >= MAX_FILES_PER_THREAD) {
            futures.add(handleFiles(toInspectFiles));
            
            toInspectFiles = new LinkedList<File>();
            toInspectSize = 0;
          }
        }
      }
      
      if (containsDirectory) {
        // might as well free up some memory
        FileUtils.purgeContentsFromCache(directory);
      }
    }
    
    if (! toInspectFiles.isEmpty()) {
      futures.add(handleFiles(toInspectFiles));
    }
    if (! toInspectDirectories.isEmpty()) {
      crawlDirectories(toInspectDirectories, futures);
    }
  }
  
  private Future<?> handleFiles(final List<File> fileList) {
    return scheduler.submit(new Runnable() {
      @Override
      public void run() {
        Iterator<File> it = fileList.iterator();
        while (it.hasNext()) {
          File f = it.next();
          Iterator<FileListenerInterface> lIt = listeners.iterator();
          while (lIt.hasNext()) {
            try {
              lIt.next().handleFile(f);
            } catch (Exception e) {
              ExceptionUtils.handleException(e);
            }
          }
        }
      }
    });
  }
}
