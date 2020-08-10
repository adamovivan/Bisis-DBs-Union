package util;

import union.MergeType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {

  private static final String DATE_TIME_FORMAT = "dd.MM.yyyy. HH:mm:ss";

  private MergeType mergeType;
  private DateTimeFormatter dateTimeFormatter;
  private String task;

  private Logger() {}

  public Logger(MergeType mergeType, String task) {
    this.mergeType = mergeType;
    this.dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);
    this.task = task;
  }

  public void info(String text) {
    System.out.println("[" + task + "] [" + mergeType.name() + "] [" + LocalDateTime.now().format(dateTimeFormatter) + "] " + text);
  }

  public void err(String text) {
    System.err.println("[" + task + "] [" + mergeType.name() + "] " + "[" + LocalDateTime.now().format(dateTimeFormatter) + "] " + text);
  }

  public static void separator() {
    System.out.println("\n-------------------------------------------------");
  }

  public static void separatorBold() {
    System.out.println("\n#################################################");
  }

  public void newLine(){
    System.out.println();
  }

  public void newLine(int n) {
    for (int i = 0; i < n; i++) {
      System.out.println();
    }
  }
}
