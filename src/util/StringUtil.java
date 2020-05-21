package util;

public class StringUtil {
  private StringUtil(){}

  public static String removeDashes(String text){
    return text.replace("-", "");
  }
}
