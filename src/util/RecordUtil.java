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

  public static String createRecordKey(Record record) {
    String isbn = record.getISBN();
    String issn = record.getISSN();
    String title = record.getTitle();
    String publisher = record.getPublisher();
    String author = record.getAuthor();
    String releaseYear = record.getReleaseYear();

    String empty = "_";

    String isbnTransformed = isbn == null ? empty : StringUtil.transformISBN(isbn);
    String issnTransformed = issn == null ? empty : StringUtil.transformISSN(issn);
    String titleTransformed = title == null ? empty : StringUtil.toLowercaseLatin(title);
    String publisherTransformed = publisher == null ? empty : StringUtil.toLowercaseLatin(publisher);
    String authorTransformed = author == null ? empty : StringUtil.toLowercaseLatin(author);

    String separator = "#";

    return isbnTransformed + separator + issnTransformed + separator + titleTransformed + separator + publisherTransformed + separator + authorTransformed + separator + releaseYear;
  }
}
