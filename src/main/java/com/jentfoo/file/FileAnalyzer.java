package com.jentfoo.file;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.TaskPriority;

public class FileAnalyzer {
  private static final boolean EXCLUDE_HIDDEN = true;
  
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
    
    int threadCount = Runtime.getRuntime().availableProcessors();
    final PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(threadCount, threadCount, Long.MAX_VALUE, 
                                                                              TaskPriority.High, 1000, false);
    scheduler.execute(new Runnable() {
      @Override
      public void run() {
        scheduler.prestartAllCoreThreads();
      }
    });
    try {
      FileCrawler fc = new FileCrawler(scheduler);
      
      if (EXCLUDE_HIDDEN) {
        fc.addFilter(new HiddenFileFilter());
      }
      
      FileNameInspector fni = new FileNameInspector();
      fc.addListener(fni);
      
      DuplicateFileInspector dfi = new DuplicateFileInspector();
      fc.addListener(dfi);
      
      // blocks till computation is done
      fc.crawlDirectories(examineDirectories);

      List<File> renameFiles = fni.getNotableFiles();
      String duplicateResult = dfi.getDuplicateAnalysis(scheduler);
      {
        if (! renameFiles.isEmpty()) {
          System.out.println();
          
          System.out.println("Files for possible rename: ");
          Iterator<File> it = renameFiles.iterator();
          while (it.hasNext()) {
            System.out.println(it.next().getAbsolutePath());
          }
        }
      }
      {
        if (! duplicateResult.isEmpty()) {
          if (! renameFiles.isEmpty()) {
            System.out.println();
          }
          System.out.println(duplicateResult);
        }
      }
      
      System.out.println("\nDONE!!");
    } finally {
      scheduler.shutdown();
    }
  }
}
