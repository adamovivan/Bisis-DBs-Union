package util;

import union.MergeType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {

  private static final String DATE_TIME_FORMAT = "dd.MM.yyyy. HH:mm:ss";

  private String database;
  private MergeType mergeType;
  private DateTimeFormatter dateTimeFormatter;

  private Logger() {}

  public Logger(String database, MergeType mergeType) {
    this.database = database;
    this.mergeType = mergeType;
    this.dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);
  }

  public void info(String text) {
    System.out.println("[" + database.toUpperCase() + "] [" + mergeType.name() + "] " + "[" + LocalDateTime.now().format(dateTimeFormatter) + "] " + text);
  }

  public void err(String text) {
    System.err.println("[" + database.toUpperCase() + "] [" + LocalDateTime.now().format(dateTimeFormatter) + "] " + text);
  }

  public void separator() {
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
