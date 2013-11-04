package com.jentfoo.file;

import java.io.File;

public class LargeFileFilter implements FileFilterInterface {
  private final long fileSizeLimit;
  
  public LargeFileFilter(long fileSizeLimit) {
    this.fileSizeLimit = fileSizeLimit;
  }
  
  @Override
  public boolean shouldExclude(File file) {
    return file.length() > fileSizeLimit;
  }
}
