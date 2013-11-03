package com.jentfoo.file;

import java.io.File;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.threadly.concurrent.PrioritySchedulerInterface;
import org.threadly.util.ExceptionUtils;

public class FileCrawler {
  private static final short MAX_FILES_PER_THREAD = 100;
  private static final long MAX_SIZE_PER_THREAD = 1024L * 1024L * 1024L * 10; // 10 GB
  
  private final PrioritySchedulerInterface scheduler;
  private final List<FileListenerInterface> listeners;
  
  public FileCrawler(PrioritySchedulerInterface scheduler) {
    this.scheduler = scheduler;
    this.listeners = new LinkedList<FileListenerInterface>();
  }
  
  public void addListener(FileListenerInterface listener) {
    listeners.add(listener);
  }

  public void crawlDirectories(List<File> examineDirectories) {
    List<Future<?>> futures = new LinkedList<Future<?>>();
    
    crawlDirectories(examineDirectories, futures);
    System.out.println("Processing " + futures.size() + " work units\n");
    
    // block till all computation has completed
    float doneCount = 0;
    int lastReportedDonePercent = 0;
    Iterator<Future<?>> it = futures.iterator();
    while (it.hasNext()) {
      try {
        Future<?> f = it.next();
        if (! f.isDone()) {
          int donePercent = (int)Math.round((doneCount / futures.size()) * 100);
          if (donePercent != lastReportedDonePercent) {
            lastReportedDonePercent = donePercent;
            System.out.println("Progress: " + donePercent + "%");
          }
          f.get();
        }
        doneCount++;
      } catch (InterruptedException e) {
        ExceptionUtils.handleException(e);
        return; // let thread exit
      } catch (ExecutionException e) {
        ExceptionUtils.handleException(e);
      }
    }
  }
  
  private void crawlDirectories(List<File> examineDirectories, List<Future<?>> futures) {
    List<File> toInspectDirectories = new LinkedList<File>();
    long toInspectSize = 0;
    List<File> toInspectFiles = new LinkedList<File>();
    Iterator<File> it = examineDirectories.iterator();
    while (it.hasNext()) {
      File directory = it.next();
      File[] contents = directory.listFiles();
      
      for (File f : contents) {
        if (f.isDirectory()) {
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
