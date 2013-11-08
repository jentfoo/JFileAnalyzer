package com.jentfoo.file;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class FileUtils {
  private static final ConcurrentMap<File, File[]> fileContentsCache;
  
  static {
    fileContentsCache = new ConcurrentHashMap<File, File[]>();
  }
  
  public static File[] getFolderContents(File folder) {
    File[] result = fileContentsCache.get(folder);
    if (result == null) {
      result = folder.listFiles();
      
      fileContentsCache.put(folder, result);
    }
    
    return result;
  }
  
  public static void purgeContentsFromCache(File folder) {
    fileContentsCache.remove(folder);
  }
}
