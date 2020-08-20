package union;

import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.nor;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;
import static util.Constants.*;

import lombok.SneakyThrows;
import org.bson.conversions.Bson;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class Queries {

  private Queries() {}

  public static Bson queryIsbn() {
      return elemMatch(
                FIELDS,
                and(
                    eq(NAME, _010),
                    elemMatch(
                        SUBFIELDS,
                        and(
                            eq(NAME, _a)
                        )
                    )
                )
            );
  }

  public static Bson queryIssnNotIsbn() {
      return and(
              not(
                  elemMatch(
                      FIELDS,
                      and(eq(NAME, _010),
                          elemMatch(
                              SUBFIELDS,
                              and(eq(NAME, _a))
                          )
                      )
                  )
              ),
              elemMatch(
                  FIELDS,
                  and(
                      eq(NAME, _011),
                      elemMatch(
                          SUBFIELDS,
                          and(eq(NAME, _a))
                      )
                  )
              )
          );
  }

  public static Bson queryTitleNotIsbnNotIssn() {
      return and(
              nor(
                  elemMatch(
                      FIELDS,
                      and(
                          eq(NAME, _010),
                          elemMatch(
                              SUBFIELDS,
                              and(
                                  eq(NAME, _a)
                              )
                          )
                      )
                  ),
                  elemMatch(
                      FIELDS,
                      and(
                          eq(NAME, _011),
                          elemMatch(
                              SUBFIELDS,
                              and(
                                  eq(NAME, _a)
                              )
                          )
                      )
                  )
              ),
              elemMatch(
                FIELDS,
                and(
                    eq(NAME, _200),
                    elemMatch(
                        SUBFIELDS,
                        and(
                            eq(NAME, _a)
                        )
                    )
                )
              )
          );
  }

  public static Bson queryNotIsbnNotIssnNotTitle() {
      return nor(
              elemMatch(
                  FIELDS,
                  and(
                      eq(NAME, _010),
                      elemMatch(
                          SUBFIELDS,
                          and(
                              eq(NAME, _a)
                          )
                      )
                  )
              ),
              elemMatch(
                  FIELDS,
                  and(
                      eq(NAME, _011),
                      elemMatch(
                          SUBFIELDS,
                          and(
                              eq(NAME, _a)
                          )
                      )
                  )
              ),
              elemMatch(
                  FIELDS,
                  and(
                      eq(NAME, _200),
                      elemMatch(
                          SUBFIELDS,
                          and(
                              eq(NAME, _a)
                          )
                      )
                  )
              )
            );
  }

  @SneakyThrows
  public static Bson queryCreationDate(String date) {
    DateFormat format = new SimpleDateFormat(DATE_FORMAT, Locale.ENGLISH);
    return gt(CREATION_DATE, format.parse(date));
  }

  public static Bson queryMergeKey(String mergeKey) {
    return eq(MERGE_KEY, mergeKey);
  }

  public static Bson queryMergeKeyNotDbName(String dbName,String mergeKey) {
    return and(
        eq(MERGE_KEY, mergeKey),
        or(
            elemMatch(
                DUPLICATES,
                and(
                    eq(NAME, dbName)
                )
            ),
            and(
                eq(CAME_FROM, dbName)
            )
        )
    );
  }

//  public static Bson queryMergeKeyNotDbName(String dbName,String mergeKey) {
//    return and(
//        eq(MERGE_KEY, mergeKey),
//        nor(
//            elemMatch(
//                DUPLICATES,
//                and(
//                    eq(NAME, dbName)
//                )
//            ),
//            and(
//                eq(CAME_FROM, dbName)
//            )
//        )
//    );
//  }

  public static Bson queryMergeKeyNotDbNameNotOriginRecordId(String dbName, Integer originRecordId, String mergeKey) {
    return and(
            eq(MERGE_KEY, mergeKey),
            nor(
              elemMatch(
                  DUPLICATES,
                  and(
                      eq(NAME, dbName),
                      eq(ORIGIN_RECORD_ID, originRecordId)
                  )
              ),
              and(
                  eq(CAME_FROM, dbName),
                  eq(ORIGIN_RECORD_ID, originRecordId)
              )
            )
    );
  }
}
