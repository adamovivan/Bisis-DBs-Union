package full_mode;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;
import records.Record;
import records.TempRecord;
import union.Union;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
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

    Bson queryISBN = elemMatch(FIELDS, and(eq(NAME, _010), elemMatch(SUBFIELDS, and(eq(NAME, _a)))));   // get all which have isbn

    MongoCursor<Record> bgbCursor = bgbCollection.find(queryISBN).cursor();
    MongoCursor<Record> gbnsCursor = gbnsCollection.find(queryISBN).cursor();
    //    MongoCursor<Record> bsCursor = bgbCollection.find(query).cursor();
    //    MongoCursor<Record> bmbCursor = bgbCollection.find(query).cursor();

    long keysAddingStart = System.currentTimeMillis();
    System.out.println("\nAdding keys, inserting bgb records -> START");
    Map<String, Integer> bgbRecordKeysISBN = new HashMap<>();  // (isbn, recordID)  // TODO use redis instead of that

    // inserts in union bgb records and remember their isbn-s
    int cnt = 1;
    while (bgbCursor.hasNext()) {
      Record bgbRecord = bgbCursor.next();
      bgbRecord.setCameFrom(BGB);
      bgbRecord.setDuplicates(new ArrayList<>());
      bgbRecordKeysISBN.put(removeDashes(bgbRecord.getISBN()), bgbRecord.getRecordID());
      unionCollection.insertOne(bgbRecord);

      if (cnt >= 1000) {    // insert first 1000 bgb records
        break;
      }
      cnt += 1;
    }
    long keysAddingEnd = System.currentTimeMillis();
    long keysAddingTimeElapsed = keysAddingEnd - keysAddingStart;
    System.out.println("Adding keys, inserting bgb records -> END -> Time: " + keysAddingTimeElapsed);

    long mergeStart = System.currentTimeMillis();
    System.out.println("\nMerging records -> START");
    int updateCnt = 0;
    int newRecordsCnt = 0;
    while (gbnsCursor.hasNext()) {
      Record gbnsRecord = gbnsCursor.next();
      String isbnGbnsRecord = gbnsRecord.getISBN();

      Integer recordIdBgb = bgbRecordKeysISBN.get(removeDashes(isbnGbnsRecord));

      if (recordIdBgb == null) {
        // exists in gbns, but not in bgb

        gbnsRecord.setCameFrom(GBNS);
        Union.setDefaultMetadata(gbnsRecord);

        unionCollection.insertOne(gbnsRecord);

        newRecordsCnt += 1;
        continue;
      }

      Bson queryUnionRecord = eq(RECORD_ID, recordIdBgb);
      MongoCursor<Record> unionRecordsCursor = bgbCollection.find(queryUnionRecord).cursor();

      if (bgbCollection.countDocuments(queryUnionRecord) > 1) {
        System.out.println("NOT UNIQUE !! Record id: " + recordIdBgb);
        break;
      }
      else {
        // exists in both gbns and bgb
        Record unionRecord = unionRecordsCursor.next();

        Union.mergeRecords(unionRecord, gbnsRecord);
        Union.setDefaultMetadata(unionRecord);
        Union.addDuplicate(unionRecord, GBNS, gbnsRecord.getRn());

        unionCollection.updateOne(eq(RECORD_ID, unionRecord.getRecordID()), Union.getUpdates(unionRecord));

        updateCnt += 1;
      }
    }

    long mergeEnd = System.currentTimeMillis();
    long mergeTimeElapsed = mergeEnd - mergeStart;
    System.out.println("Merging records -> END -> Time: " + mergeTimeElapsed);
    System.out.println("\nGBNS has isbn: " + gbnsCollection.countDocuments(queryISBN));
    System.out.println("Union update: " + updateCnt);
    System.out.println("Union new records: " + newRecordsCnt);

  }
}
