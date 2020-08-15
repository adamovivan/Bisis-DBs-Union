package util;

import union.MergeType;

import static util.Constants.ISBN_978_LENGTH;
import static util.Constants.ISBN_PREFIX_978_LENGTH;
import static util.Constants.ISBN_PREFIX_978;

public class StringUtil {
  private StringUtil() {}

  public static String transformMergeKey(String text, MergeType mergeType) {
    String mergeKey = text.replace("-", "");

    if (mergeType == MergeType.ISBN && mergeKey.length() == ISBN_978_LENGTH && mergeKey.startsWith(ISBN_PREFIX_978)) {
      return mergeKey.substring(ISBN_PREFIX_978_LENGTH, ISBN_978_LENGTH);
    }

    return mergeKey;
  }
}
