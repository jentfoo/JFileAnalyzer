package com.jentfoo.file;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class FileNameInspector implements FileListenerInterface {
  private final static List<String> NOTABLE_CONTENTS;
  
  static {
    // should be only in lower case
    List<String> contents = new ArrayList<String>(8);
    contents.add("xvid");
    contents.add("hdtv");
    contents.add("dvdrip");
    contents.add("2hd");
    contents.add("x264");
    contents.add("axxo");
    contents.add("fxg");
    contents.add("momentum");
    
    NOTABLE_CONTENTS = Collections.unmodifiableList(contents);
  }
  
  private final List<File> notableFiles;
  
  public FileNameInspector() {
    notableFiles = new LinkedList<File>();
  }
  
  public List<File> getNotableFiles() {
    return notableFiles;
  }
  
  @Override
  public void handleFile(File file) {
    if (isNotableName(file)) {
      notableFiles.add(file);
    }
  }
  
  private boolean isNotableName(File file) {
    String fileName = file.getName().toLowerCase();
    Iterator<String> it = NOTABLE_CONTENTS.iterator();
    while (it.hasNext()) {
      if (fileName.contains(it.next())) {
        return true;
      }
    }
    
    return false;
  }
}
