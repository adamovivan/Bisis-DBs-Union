package union;

import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.nor;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.eq;
import org.bson.conversions.Bson;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static util.Constants.CONTENT;
import static util.Constants.FIELDS;
import static util.Constants.LAST_MODIFIED_DATE;
import static util.Constants.NAME;
import static util.Constants.SUBFIELDS;
import static util.Constants._200;
import static util.Constants._a;
import static util.Constants._010;
import static util.Constants._011;


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

  public static Bson queryGreaterThanDate(String date) {
    DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH);
    try {
      return gt(LAST_MODIFIED_DATE, format.parse(date));
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static Bson queryByOriginAndRn(String origin, Integer rn) {
    return null;
  }
}
