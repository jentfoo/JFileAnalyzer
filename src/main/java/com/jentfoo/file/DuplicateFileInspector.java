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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

    Map<FolderContainer, Boolean> completeFolders = lookForDuplicatedFolders(scheduler, duplicateFiles);
    result.append(newLine);
    if (completeFolders.isEmpty()) {
      result.append("No folders are completely duplicated by others");
    } else {
      List<FolderContainer> equalFolders = new LinkedList<FolderContainer>();
      List<FolderContainer> partiallyDuplicatedFolders = new LinkedList<FolderContainer>();
      {
        Iterator<Entry<FolderContainer, Boolean>> it = completeFolders.entrySet().iterator();
        while (it.hasNext()) {
          Entry<FolderContainer, Boolean> e = it.next();
          if (e.getValue()) {
            equalFolders.add(e.getKey());
          } else {
            partiallyDuplicatedFolders.add(e.getKey());
          }
        }
      }
      if (! equalFolders.isEmpty()) {
        result.append("Folders which contents are exactly equal:").append(newLine);
        outputFolderList(result, equalFolders);
        
        if (! partiallyDuplicatedFolders.isEmpty()) {
          result.append(newLine);
        }
      }
      if (! partiallyDuplicatedFolders.isEmpty()) {
        result.append("Folders which are PARTIALLY duplicated (first folder is fully duplicated by second folder):").append(newLine);
        outputFolderList(result, partiallyDuplicatedFolders);
      }
    }
    result.append(newLine);
    result.append("Folder analysis took ")
          .append(Clock.accurateTime() - startTime)
          .append("ms to process");
    
    return result.toString();
  }
  
  private static void outputFolderList(StringBuilder sb, List<FolderContainer> folders) {
    Iterator<FolderContainer> it = folders.iterator();
    while (it.hasNext()) {
        FolderContainer fc = it.next();
        sb.append(fc.folder1).append('\t').append(fc.folder2)
          .append('\n');
    }
  }
  
  private Map<FolderContainer, Boolean> lookForDuplicatedFolders(PrioritySchedulerInterface scheduler, 
                                                                 List<List<File>> duplicateFiles) {
    Map<FolderContainer, Boolean> completeFolders = new HashMap<FolderContainer, Boolean>();

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
      DigestResult dr = makeFileDigest(file);
      
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
  
  private DigestResult makeFileDigest(File file) throws IOException, 
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
    private final Map<FolderContainer, Boolean> completeFolders;
    
    private DuplicateFolderExaminer(boolean isConcurrent, 
                                    List<List<File>> threadFiles, 
                                    Map<FolderContainer, Boolean> completeFolders) {
      this.isConcurrent = isConcurrent;
      this.threadFiles = threadFiles;
      this.completeFolders = completeFolders;
    }
    
    @Override
    public void run() {
      Iterator<List<File>> it = threadFiles.iterator();
      while (it.hasNext()) {
        List<File> files = it.next();
        lookForDuplicatedFolders(isConcurrent, files, 
                                 completeFolders);
      }
    }
    
    private void lookForDuplicatedFolders(boolean threadSafe, List<File> dupFiles, 
                                          Map<FolderContainer, Boolean> completeFolders) {
      Iterator<File> firstIt = dupFiles.iterator();
      while (firstIt.hasNext()) {
        File firstFile = firstIt.next();
        File firstParent = firstFile.getParentFile();
        File[] parentContents = FileUtils.getFolderContents(firstParent);
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
            File nextDupParent = nextDupFile.getParentFile();

            // skip if the parent folders are the same
            if (firstParent.getAbsolutePath().equals(nextDupParent.getAbsolutePath())) {
              continue;
            }
            FolderContainer possibleContainer = new FolderContainer(firstParent, nextDupParent);
            
            if (threadSafe) {
              synchronized (completeFolders) {
                if (completeFolders.containsKey(possibleContainer)) {
                  continue;
                }
              }
            } else {
              if (completeFolders.containsKey(possibleContainer)) {
                continue;
              }
            }
            
            File[] nextDupParentContents = FileUtils.getFolderContents(nextDupParent);
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
              FolderContainer reverseContainer = new FolderContainer(possibleContainer.folder2, 
                                                                     possibleContainer.folder1);
              if (threadSafe) {
                synchronized (completeFolders) {
                  if (completeFolders.containsKey(reverseContainer)) {
                    completeFolders.put(reverseContainer, true);
                  } else if (! completeFolders.containsKey(possibleContainer)) {
                    completeFolders.put(possibleContainer, false);
                  }
                }
              } else {
                if (completeFolders.containsKey(reverseContainer)) {
                    completeFolders.put(reverseContainer, true);
                } else { // no need to check again
                  completeFolders.put(possibleContainer, false);
                }
              }
            }
          }
        }
      }
    }
  }
}
