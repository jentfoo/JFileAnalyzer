package com.jentfoo.file;

import java.io.File;

public class HiddenFileFilter implements FileFilterInterface {
  private static final String HIDDEN_STR = "/.";
  
  @Override
  public boolean shouldExclude(File file) {
    String path = file.getPath();
    boolean isHidden = false;
    int hiddenIndex = -1;
    do {
      hiddenIndex = path.indexOf(HIDDEN_STR, hiddenIndex + 1);
      if (hiddenIndex > 0 && hiddenIndex + HIDDEN_STR.length() + 1 < path.length()) {
        if (path.charAt(hiddenIndex + HIDDEN_STR.length() + 1) != '/') {
          isHidden = true;
          break;
        }
      }
    } while (hiddenIndex > 0);
    
    return isHidden;
  }
}
