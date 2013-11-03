package com.jentfoo.file;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.TaskPriority;

public class FileAnalyzer {
  public static void main(String args[]) {
    if (args.length == 0) {
      System.err.println("Must provide at least one valid path to inspect");
      System.exit(1);
    }
    
    List<File> examineDirectories = new ArrayList<File>(args.length);
    for (String path : args) {
      File toInspectPath = new File(path);
      if (! toInspectPath.exists()) {
        throw new IllegalArgumentException("Path does not exist: " + path);
      } else if (! toInspectPath.isDirectory()) {
        throw new IllegalArgumentException("Path is not a directory: " + path);
      }
      
      examineDirectories.add(toInspectPath);
    }
    
    int threadCount = Runtime.getRuntime().availableProcessors() * 2;
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(threadCount, threadCount, Long.MAX_VALUE, 
                                                                        TaskPriority.High, 1000, false);
    scheduler.prestartAllCoreThreads();
    try {
      FileCrawler fc = new FileCrawler(scheduler);
      fc.crawlDirectories(examineDirectories);
    } finally {
      scheduler.shutdown();
    }
  }
}
