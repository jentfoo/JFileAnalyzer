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
    
    // block till all computation has completed
    float doneCount = 0;
    Iterator<Future<?>> it = futures.iterator();
    while (it.hasNext()) {
      try {
        it.next().get();
        doneCount++;
        System.out.println("Progress: " + ((doneCount / futures.size()) * 100) + "%");
      } catch (InterruptedException e) {
        return;
      } catch (ExecutionException e) {
        ExceptionUtils.handleException(e);
      }
    }
  }
  
  private void crawlDirectories(List<File> examineDirectories, List<Future<?>> futures) {
    List<File> toInspectDirectories = new LinkedList<File>();
    List<File> toInspectFiles = new LinkedList<File>();
    Iterator<File> it = examineDirectories.iterator();
    while (it.hasNext()) {
      File[] contents = it.next().listFiles();
      
      for (File f : contents) {
        if (f.isDirectory()) {
          toInspectDirectories.add(f);
        } else {
          toInspectFiles.add(f);
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
