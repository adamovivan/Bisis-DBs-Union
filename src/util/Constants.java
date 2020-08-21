package util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class Constants {

  private Constants() {}

  public static final String MONGO_CONNECTION_URL = "mongodb://localhost:27017";
  public static final String REDIS_HOST = "localhost";
  public static final int REDIS_PORT = 6379;
  public static final int REDIS_TIMEOUT = 120000;
  public static final String DATABASE_NAME = "bisis";
  public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

  public static final String BGB = "bgb";
  public static final String GBNS = "gbns";
  public static final String BS = "bs";
  public static final String BMB = "bmb";
  public static final String MSK = "msk";

  public static final String UNION_RECORDS = "a_test_union_records";
  public static final String BGB_RECORDS = "bgb_records";
  public static final String GBNS_RECORDS = "gbns_records";
  public static final String BS_RECORDS = "bs_records";
  public static final String BMB_RECORDS = "bmb_records";
  public static final String MSK_RECORDS = "msk_records";

  public static final char _a = 'a';
  public static final char _b = 'b';
  public static final char _c = 'c';
  public static final char _d = 'd';
  public static final String _010 = "010";
  public static final String _011 = "011";
  public static final String _200 = "200";
  public static final String _210 = "210";
  public static final String _700 = "700";

  public static final String FIELDS = "fields";
  public static final String SUBFIELDS = "subfields";
  public static final String NAME = "name";
  public static final String CONTENT = "content";
  public static final String RECORD_ID = "recordID";
  public static final String DUPLICATES = "duplicates";
  public static final String CREATOR = "creator";
  public static final String CREATION_DATE = "creationDate";
  public static final String MODIFIER = "modifier";
  public static final String LAST_MODIFIED_DATE = "lastModifiedDate";
  public static final String ORIGIN_RECORD_ID = "originRecordID";
  public static final String CAME_FROM = "cameFrom";
  public static final String MERGE_KEY = "mergeKey";

  public static final String ISBN_PREFIX_978 = "978";
  public static final int ISBN_PREFIX_978_LENGTH = 3;
  public static final int ISBN_978_LENGTH = 13;

}
