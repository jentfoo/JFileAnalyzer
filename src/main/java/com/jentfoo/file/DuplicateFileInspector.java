package com.jentfoo.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.threadly.util.ExceptionUtils;

public class DuplicateFileInspector implements FileListenerInterface {
  private static final String ALGORITHM = "SHA-256";
  
  private final ConcurrentMap<DigestResult, List<File>> fileDigests;
  
  public DuplicateFileInspector() {
    fileDigests = new ConcurrentHashMap<DigestResult, List<File>>();
  }
  
  public List<List<File>> getDuplicateFiles() {
    List<List<File>> result = new LinkedList<List<File>>();
    
    Iterator<List<File>> it = fileDigests.values().iterator();
    while (it.hasNext()) {
      List<File> l = it.next();
      if (l.size() > 1) {
        result.add(l);
      }
    }
    
    return result;
  }

  @Override
  public void handleFile(File file) {
    try {
      DigestResult dr = getFileDigest(file);
      
      // duplicates should be rare, so we assume there is none
      List<File> dupFiles = new LinkedList<File>();
      List<File> existingList = fileDigests.putIfAbsent(dr, dupFiles);
      if (existingList != null) {
        dupFiles = existingList;
      }
      dupFiles.add(file);
    } catch (Exception e) {
      ExceptionUtils.handleException(e);
    }
  }
  
  private DigestResult getFileDigest(File file) throws IOException, 
                                                       NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance(ALGORITHM);
    InputStream in = new FileInputStream(file);
    try {
      byte[] buffer = new byte[8192];
      int readCount;
      while ((readCount = in.read(buffer)) > -1) {
        md.update(buffer, 0, readCount);
      }

      byte[] digest = md.digest();
      return new DigestResult(digest);
    } finally {
      in.close();
    }
  }
  
  private static class DigestResult {
    private final byte[] bytes;
    private final int hashCode;
    
    private DigestResult(byte[] bytes) {
      this.bytes = bytes;
      this.hashCode = Arrays.hashCode(bytes);
    }
    
    @Override
    public int hashCode() {
      return hashCode;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof DigestResult) {
        DigestResult dr = (DigestResult)o;
        
        return MessageDigest.isEqual(bytes, dr.bytes);
      } else {
        return false;
      }
    }
  }
}
