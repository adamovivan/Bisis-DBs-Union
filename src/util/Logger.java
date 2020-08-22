package util;

import union.MergeType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {

  private final DateTimeFormatter dateTimeFormatter;

  private String database;
  private MergeType mergeType;
  private BufferedWriter executionWriter;

  public Logger(String fileName) {
    dateTimeFormatter = DateTimeFormatter.ofPattern(Constants.DATE_TIME_FORMAT);
    try {
      executionWriter = new BufferedWriter(new FileWriter(Constants.LOGS_PATH + "/" + fileName, true));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void setDatebaseAndMergeType(String database, MergeType mergeType) {
    this.database = database;
    this.mergeType = mergeType;
  }

  public void mergeInfo(String text) {
    String message = "[" + database.toUpperCase() + "] [" + mergeType.name() + "] " + "[" + LocalDateTime.now().format(dateTimeFormatter) + "] " + text;
    System.out.println(message);
    writeToFile(message);
  }

  public void info(String text) {
    System.out.println(text);
    writeToFile(text);
  }

  public void separator() {
    System.out.println("\n#################################################");
    writeToFile("\n#################################################");
  }

  public void newLine(){
    System.out.println();
    writeToFile("");
  }

  public void writeToFile(String text) {
    try {
      executionWriter.write(text+"\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void closeWriter() {
    try {
      executionWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
