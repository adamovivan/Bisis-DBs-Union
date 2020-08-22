package union;

import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.nor;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.eq;
import static util.Constants.*;

import org.bson.conversions.Bson;
import java.time.LocalDateTime;

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

  public static Bson queryCreationDate(String date) {
    return gt(CREATION_DATE, LocalDateTime.parse(date));
  }

  public static Bson queryMergeKey(String mergeKey) {
    return eq(MERGE_KEY, mergeKey);
  }

}
