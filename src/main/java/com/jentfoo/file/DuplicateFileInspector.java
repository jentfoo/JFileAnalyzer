package com.jentfoo.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import org.threadly.concurrent.PrioritySchedulerInterface;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

public class DuplicateFileInspector implements FileListenerInterface {
  private static final String ALGORITHM = "SHA-256";
  private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();
  private static final int DUPLICATE_FOLDER_THREADING_THRESHOLD = Math.max(CPU_COUNT, 50);
  
  private final ConcurrentMap<DigestResult, List<File>> digestToFile;
  private final ConcurrentMap<File, DigestResult> fileToDigest;
  
  public DuplicateFileInspector() {
    digestToFile = new ConcurrentHashMap<DigestResult, List<File>>();
    fileToDigest = new ConcurrentHashMap<File, DigestResult>();
  }
  
  public List<List<File>> getDuplicateFiles() {
    List<List<File>> result = new LinkedList<List<File>>();
    
    Iterator<List<File>> it = digestToFile.values().iterator();
    while (it.hasNext()) {
      List<File> l = it.next();
      if (l.size() > 1) {
        result.add(l);
      }
    }
    
    return result;
  }
  
  public String getDuplicateAnalysis(PrioritySchedulerInterface scheduler) {
    long startTime = Clock.accurateTime();
    char newLine = '\n';
    List<List<File>> duplicateFiles = getDuplicateFiles();
    StringBuilder result = new StringBuilder();
    if (duplicateFiles.isEmpty()) {
      return result.toString();
    }
    
    result.append("Duplicate files: ").append(newLine);
    
    Iterator<List<File>> dupIt = duplicateFiles.iterator();
    while (dupIt.hasNext()) {
      List<File> dupFiles = dupIt.next();
      result.append(dupFiles.toString()).append(newLine);
    }

    Collection<FolderContainer> completeFolders = lookForDuplicatedFolders(scheduler, duplicateFiles);
    result.append(newLine);
    if (completeFolders.isEmpty()) {
      result.append("No folders are completely duplicated by others");
    } else {
      Iterator<FolderContainer> it = completeFolders.iterator();
      while (it.hasNext()) {
        FolderContainer fc = it.next();
        result.append("Folder ").append(fc.folder1)
              .append(" is completely duplicated in folder ").append(fc.folder2)
              .append(newLine);
      }
    }
    result.append(newLine);
    result.append("Folder analysis took ")
          .append(Clock.accurateTime() - startTime)
          .append("ms to process");
    
    return result.toString();
  }
  
  private Collection<FolderContainer> lookForDuplicatedFolders(PrioritySchedulerInterface scheduler, 
                                                               List<List<File>> duplicateFiles) {
    final Set<FolderContainer> completeFolders = new HashSet<FolderContainer>();

    System.out.println("\nDoing folder analysis for duplicate count of: " + duplicateFiles.size());
    if (duplicateFiles.size() < DUPLICATE_FOLDER_THREADING_THRESHOLD) {
      new DuplicateFolderExaminer(false, duplicateFiles, 
                                  completeFolders).run();
    } else {  // there is enough analysis to do, might as well make it multi-threaded
      int taskCount;
      if (duplicateFiles.size() > 5000) {
        taskCount = Math.min(CPU_COUNT * 10, 100);
      } else if (duplicateFiles.size() > DUPLICATE_FOLDER_THREADING_THRESHOLD * 2) {
        taskCount = CPU_COUNT * 2;
      } else {  // if we are barely above our threading threshold, no need to submit more tasks
        taskCount = CPU_COUNT;
      }
      
      int listsPerThread = duplicateFiles.size() / taskCount;
      
      List<Future<?>> futures = new ArrayList<Future<?>>(taskCount);
      int previousEndIndex = 0;
      for (int i = 0; i < taskCount; i++) {
        final List<List<File>> threadFiles;
        if (i == taskCount - 1) {
          threadFiles = duplicateFiles.subList(previousEndIndex, 
                                               duplicateFiles.size());
        } else {
          threadFiles = duplicateFiles.subList(previousEndIndex, 
                                               previousEndIndex + listsPerThread);
        }
        Future<?> f = scheduler.submit(new DuplicateFolderExaminer(true, threadFiles, 
                                                                   completeFolders));
        
        futures.add(f);
      }
      
      // block till all have completed
      FutureUtil.blockTillAllDone(futures);
    }
    
    return completeFolders;
  }

  @Override
  public void handleFile(File file) {
    try {
      DigestResult dr = getFileDigest(file);
      
      // duplicates should be rare, so we assume there is none
      List<File> dupFiles = new LinkedList<File>();
      List<File> existingList = digestToFile.putIfAbsent(dr, dupFiles);
      if (existingList != null) {
        dupFiles = existingList;
      }
      dupFiles.add(file);
      fileToDigest.put(file, dr);
    } catch (Exception e) {
      if (e instanceof FileNotFoundException) {
        // ignore
      } else {
        ExceptionUtils.handleException(e);
      }
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
  
  private static class FolderContainer {
    private final File folder1;
    private final File folder2;
    private final int hashCode;
    
    private FolderContainer(File folder1, File folder2) {
      this.folder1 = folder1;
      this.folder2 = folder2;
      this.hashCode = folder1.hashCode() ^ folder2.hashCode();
    }
    
    @Override
    public int hashCode() {
      return hashCode;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof FolderContainer) {
        FolderContainer fc = (FolderContainer)o;
        return fc.folder1.equals(folder1) && 
                 fc.folder2.equals(folder2);
      } else {
        return false;
      }
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
  
  private class DuplicateFolderExaminer implements Runnable {
    private final boolean isConcurrent;
    private final List<List<File>> threadFiles;
    private final Collection<FolderContainer> completeFolders;
    private final Map<File, File[]> fileContentsCache;
    
    private DuplicateFolderExaminer(boolean isConcurrent, 
                                    List<List<File>> threadFiles, 
                                    Collection<FolderContainer> completeFolders) {
      this.isConcurrent = isConcurrent;
      this.threadFiles = threadFiles;
      this.completeFolders = completeFolders;
      fileContentsCache = new HashMap<File, File[]>();
    }
    
    @Override
    public void run() {
      Iterator<List<File>> it = threadFiles.iterator();
      while (it.hasNext()) {
        List<File> files = it.next();
        try {
          lookForDuplicatedFolders(isConcurrent, files, 
                                   completeFolders);
        } catch (IOException e) {
          System.err.println("Error examining file list: " + files + 
                               "...exception: " + e.getMessage());
        }
      }
    }
    
    private File[] getFolderContents(File folder) {
      File[] result = fileContentsCache.get(folder);
      if (result == null) {
        result = folder.listFiles();
        
        fileContentsCache.put(folder, result);
      }
      
      return result;
    }
    
    private void lookForDuplicatedFolders(boolean threadSafe, List<File> dupFiles, 
                                          Collection<FolderContainer> completeFolders) throws IOException {
      Iterator<File> firstIt = dupFiles.iterator();
      while (firstIt.hasNext()) {
        File firstFile = firstIt.next();
        File firstParent = firstFile.getParentFile().getCanonicalFile();
        File[] parentContents = getFolderContents(firstParent);
        boolean isFilesOnly = true;
        for (File f : parentContents) {
          if (f.isDirectory()) {
            isFilesOnly = false;
            break;
          }
        }
        if (isFilesOnly) {
          Iterator<File> fIt = dupFiles.iterator();
          while (fIt.hasNext()) {
            File nextDupFile = fIt.next();
            if (nextDupFile == firstFile) { // skip the same file
              continue;
            }
            File nextDupParent = nextDupFile.getParentFile().getCanonicalFile();

            // skip if the parent folders are the same
            if (firstParent.getAbsolutePath().equals(nextDupParent.getAbsolutePath())) {
              continue;
            }
            FolderContainer possibleContainer = new FolderContainer(firstParent, nextDupParent);
            
            if (threadSafe) {
              synchronized (completeFolders) {
                if (completeFolders.contains(possibleContainer)) {
                  continue;
                }
              }
            } else {
              if (completeFolders.contains(possibleContainer)) {
                continue;
              }
            }
            
            File[] nextDupParentContents = getFolderContents(nextDupParent);
            boolean allHaveMatch = true;
            for (File pf : parentContents) {
              DigestResult dr = fileToDigest.get(pf);
              boolean hasMatch = false;
              for (File npf : nextDupParentContents) {
                DigestResult dr2 = fileToDigest.get(npf);
                if (dr2 != null && dr2.equals(dr)) {
                  hasMatch = true;
                  break;
                }
              }
              if (! hasMatch) {
                allHaveMatch = false;
                break;
              }
            }
            
            if (allHaveMatch) {
              if (threadSafe) {
                synchronized (completeFolders) {
                  if (! completeFolders.contains(possibleContainer)) {
                    completeFolders.add(possibleContainer);
                  }
                }
              } else {  // no need to check again
                completeFolders.add(possibleContainer);
              }
            }
          }
        }
      }
    }
  }
}
