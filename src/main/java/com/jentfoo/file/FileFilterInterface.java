package com.jentfoo.file;

import java.io.File;

public interface FileFilterInterface {
  /**
   * Check to see if a file (and sub files if a folder) should 
   * be excluded from crawling.
   * 
   * @param file file or folder looking to inspect
   * @return true if the file should be skipped from crawling
   */
  public boolean shouldExclude(File file);
}
