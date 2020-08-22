package util;

import static util.Constants.ISBN_978_LENGTH;
import static util.Constants.ISBN_PREFIX_978_LENGTH;
import static util.Constants.ISBN_PREFIX_978;

public class StringUtil {
  private StringUtil() {}

  public static String transformISBN(String isbn) {
    isbn = isbn.replace("-", "");
    if (isbn.length() == ISBN_978_LENGTH && isbn.startsWith(ISBN_PREFIX_978)) {
      return isbn.substring(ISBN_PREFIX_978_LENGTH, ISBN_978_LENGTH);
    }
    return isbn;
  }

  public static String transformISSN(String issn) {
    return issn.replace("-", "");
  }

  public static String toLowercaseLatin(String text) {
    return LatCyrFilterUtil.removeAccents(LatCyrFilterUtil.toLatin(text)).toLowerCase();
  }
}
