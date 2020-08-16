package union;

import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.nor;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.eq;
import org.bson.conversions.Bson;
import static util.Constants.FIELDS;
import static util.Constants.NAME;
import static util.Constants.SUBFIELDS;
import static util.Constants._200;
import static util.Constants._a;
import static util.Constants._010;
import static util.Constants._011;


public class Queries {

  private Queries() {}

  public static final Bson queryIsbn =
      elemMatch(
          FIELDS,
          and(
              eq(NAME, _010),
              elemMatch(SUBFIELDS, and(
                  eq(NAME, _a)
              )
              )
          )
      );

  public static final Bson queryIssnNotIsbn =
      and(
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

  public static Bson queryTitleNotIsbnNotIssn =
      and(
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
