package com.jentfoo.file;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.threadly.util.ExceptionUtils;

public class FutureUtil {
  public static void blockTillAllDone(List<Future<?>> futures) {
    float doneCount = 0;
    int lastReportedDonePercent = 0;
    Iterator<Future<?>> it = futures.iterator();
    while (it.hasNext()) {
      try {
        Future<?> f = it.next();
        if (! f.isDone() || ! it.hasNext()) {
          // we take * 10 and / 10 so we can get one additional decimal of accuracy
          int donePercent = (int)Math.round((doneCount / futures.size()) * 100 * 10);
          if (donePercent != lastReportedDonePercent) {
            lastReportedDonePercent = donePercent;
            System.out.println("Progress: " + (donePercent / 10.) + "%");
          }
          f.get();
        }
        doneCount++;
      } catch (InterruptedException e) {
        ExceptionUtils.handleException(e);
        return; // let thread exit
      } catch (ExecutionException e) {
        ExceptionUtils.handleException(e);
      }
    }
  }
}
