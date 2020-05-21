package full_mode;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import records.Record;
import records.TempRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static util.Constants.*;
import static util.StringUtil.*;

public class FullMode {

  private final MongoClient mongoClient;
  private final String[] collections = {BGB_RECORDS, GBNS_RECORDS, BS_RECORDS, BMB_RECORDS};

  public FullMode(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  public void start() {
    MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);

    MongoCollection<Record> unionCollection = database.getCollection(UNION_RECORDS, Record.class);
    MongoCollection<TempRecord> tempRecordCollection = database.getCollection("tempRecord", TempRecord.class);

    MongoCollection<Record> bgbCollection = database.getCollection(BGB_RECORDS, Record.class);
    MongoCollection<Record> gbnsCollection = database.getCollection(GBNS_RECORDS, Record.class);
    MongoCollection<Record> bsCollection = database.getCollection(BS_RECORDS, Record.class);
    MongoCollection<Record> bmbCollection = database.getCollection(BMB_RECORDS, Record.class);

    //    Bson query = elemMatch(FIELDS, and(eq(NAME, _010), elemMatch(SUBFIELDS, and(eq(NAME, _a), eq(CONTENT, "86-13-00157-2")))));
    Bson query = elemMatch(FIELDS, and(eq(NAME, _010), elemMatch(SUBFIELDS, and(eq(NAME, _a)))));

    MongoCursor<Record> bgbCursor = bgbCollection.find(query).cursor();
    MongoCursor<Record> gbnsCursor = bgbCollection.find(query).cursor();
    //    MongoCursor<Record> bsCursor = bgbCollection.find(query).cursor();
    //    MongoCursor<Record> bmbCursor = bgbCollection.find(query).cursor();
    System.out.println();
    long keysAddingStart = System.currentTimeMillis();
    System.out.println("Keys adding -> START");
    Map<String, Integer> bgbRecordKeys = new HashMap<>();    // TODO use redis instead of that
    int cnt = 1;
    while (bgbCursor.hasNext()) {
      Record record = bgbCursor.next();
      record.setCameFrom("bgb");
      record.setDuplicates(new ArrayList<>());
      bgbRecordKeys.put(removeDashes(record.getISBN()), record.getRecordID());
      unionCollection.insertOne(record);

      if (cnt >= 1000) {
        break;
      }
      cnt += 1;
    }
    long keysAddingEnd = System.currentTimeMillis();
    long keysAddingTimeElapsed = keysAddingEnd - keysAddingStart;
    System.out.println("Keys adding -> END -> Time: " + keysAddingTimeElapsed);
    System.out.println();


    long unionUpdateStart = System.currentTimeMillis();
    System.out.println("Keys adding -> START");
    cnt = 1;
    while (cnt < 10000 ) {
      if (!gbnsCursor.hasNext()) {
        break;
      }
      String isbn = gbnsCursor.next().getISBN();
      Bson queryBgbRecord = eq("recordID", bgbRecordKeys.get(removeDashes(isbn)));
      MongoCursor<Record> recordsCursor = bgbCollection.find(queryBgbRecord).cursor();

      if(bgbCollection.countDocuments(queryBgbRecord) > 1) {
        System.out.println("NOT UNIQUE !!");
      }
      if (!recordsCursor.hasNext()) {     // Zero or One record
        continue;
      }

      Record record = recordsCursor.next();
      if (record.getDuplicates() == null) {
        record.setDuplicates(Collections.singletonList("gbns"));
      } else {
        record.getDuplicates().add("gbns");
      }

      unionCollection.updateOne(eq("recordID", record.getRecordID()), set("duplicates", record.getDuplicates()));
      cnt += 1;
    }


    long unionUpdateEnd = System.currentTimeMillis();
    long unionUpdateTimeElapsed = unionUpdateEnd - unionUpdateStart;
    System.out.println("Keys adding -> END -> Time: " + unionUpdateTimeElapsed);

  }
}
