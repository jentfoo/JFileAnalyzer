package com.jentfoo.file;

import java.io.File;

public interface FileListenerInterface {
  /**
   * Called when ever the crawler is ready to 
   * have a file processed.  Keep in mind that 
   * any implementations of this interface may 
   * have this called in parallel.
   * 
   * @param file File to be handled
   */
  public void handleFile(File file);
}
