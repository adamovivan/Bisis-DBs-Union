package util;

import records.Record;
import union.MergeType;

public class RecordUtil {

  private RecordUtil() {}

  public static String getMergeTypeValue(Record record, MergeType mergeType) {
    if (mergeType == MergeType.ISBN) {
      return record.getISBN();
    } else {
      return record.getISSN();
    }
  }
}
