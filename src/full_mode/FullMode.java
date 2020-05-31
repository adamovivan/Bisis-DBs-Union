package full_mode;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import records.Duplicate;
import records.Record;
import records.TempRecord;
import union.Union;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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

    Bson queryISBN = elemMatch(FIELDS, and(eq(NAME, _010), elemMatch(SUBFIELDS, and(eq(NAME, _a)))));   // get all which have isbn

    MongoCursor<Record> bgbCursor = bgbCollection.find(queryISBN).cursor();
    MongoCursor<Record> gbnsCursor = gbnsCollection.find(queryISBN).cursor();
    //    MongoCursor<Record> bsCursor = bgbCollection.find(query).cursor();
    //    MongoCursor<Record> bmbCursor = bgbCollection.find(query).cursor();

    long keysAddingStart = System.currentTimeMillis();
    System.out.println("\nKeys adding -> START");
    Map<String, Integer> bgbRecordKeysISBN = new HashMap<>();  // (isbn, recordID)  // TODO use redis instead of that

    // inserts in union bgb records and remember their isbn-s
    int cnt = 1;
    while (bgbCursor.hasNext()) {
      Record bgbRecord = bgbCursor.next();
      bgbRecord.setCameFrom(BGB);
      bgbRecord.setDuplicates(new ArrayList<>());
      bgbRecordKeysISBN.put(removeDashes(bgbRecord.getISBN()), bgbRecord.getRecordID());
      unionCollection.insertOne(bgbRecord);

      if (cnt >= 1000) {
        break;
      }
      cnt += 1;
    }
    long keysAddingEnd = System.currentTimeMillis();
    long keysAddingTimeElapsed = keysAddingEnd - keysAddingStart;
    System.out.println("Keys adding -> END -> Time: " + keysAddingTimeElapsed);

    long mergeStart = System.currentTimeMillis();
    System.out.println("\nMerging records -> START");
    int update_cnt = 0;
    int new_records_cnt = 0;
    while (gbnsCursor.hasNext()) {
      Record gbnsRecord = gbnsCursor.next();
      String isbn = gbnsRecord.getISBN();
      Bson queryUnionRecord = eq(RECORD_ID, bgbRecordKeysISBN.get(removeDashes(isbn)));
      MongoCursor<Record> unionRecordsCursor = bgbCollection.find(queryUnionRecord).cursor();

      if (bgbCollection.countDocuments(queryUnionRecord) > 1) {
        System.out.println("NOT UNIQUE !!");
      }
      if (!unionRecordsCursor.hasNext()) {     // Zero or One record
        // exists in gbns, but not in bgb
        gbnsRecord.setCameFrom(GBNS);
        Union.setDefaultMetadata(gbnsRecord);

        unionCollection.insertOne(gbnsRecord);

        new_records_cnt += 1;
      } else {
        // exists in both gbns and bgb
        Record unionRecord = unionRecordsCursor.next();

        Union.mergeRecords(unionRecord, gbnsRecord);
        Union.setDefaultMetadata(unionRecord);
        Union.addDuplicate(unionRecord, GBNS, gbnsRecord.getRn());

//        unionCollection.insertOne(unionRecord);

        unionCollection.updateOne(eq(RECORD_ID, unionRecord.getRecordID()), Union.getUpdates(unionRecord));

        update_cnt += 1;
      }
    }

    long mergeEnd = System.currentTimeMillis();
    long mergeTimeElapsed = mergeEnd - mergeStart;
    System.out.println("Merging records -> END -> Time: " + mergeTimeElapsed);
    System.out.println("\nGBNS has isbn: " + gbnsCollection.countDocuments(queryISBN));
    System.out.println("Union update: " + update_cnt);
    System.out.println("Union new records: " + new_records_cnt);

//    long remainingFieldsStart = System.currentTimeMillis();
//    System.out.println("\nAdding remaining fields -> START");
//    // todo add remaining fields from gbns
//
//
//
//    long remainingFieldsEnd = System.currentTimeMillis();
//    long remainingFieldsTimeElapsed = remainingFieldsEnd - remainingFieldsStart;
//    System.out.println("Adding remaining fields -> END -> Time: " + remainingFieldsTimeElapsed);

  }

//  private void updateDuplicates(MongoCollection<Record> collection, Map<String, Integer> recordKeys, Bson query) {
//
//    int cnt = 1;
//    while (cnt < 10000 ) {
//      if (!gbnsCursor.hasNext()) {
//        break;
//      }
//      String isbn = gbnsCursor.next().getISBN();
//      Bson queryBgbRecord = eq(RECORD_ID, recordKeys.get(removeDashes(isbn)));
//      MongoCursor<Record> recordsCursor = collection.find(queryBgbRecord).cursor();
//
//      if(collection.countDocuments(queryBgbRecord) > 1) {
//        System.out.println("NOT UNIQUE !!");
//      }
//      if (!recordsCursor.hasNext()) {     // Zero or One record
//        continue;
//      }
//
//      Record record = recordsCursor.next();
//      if (record.getDuplicates() == null) {
//        record.setDuplicates(Collections.singletonList(GBNS));
//      } else {
//        record.getDuplicates().add(GBNS);
//      }
//
//      unionCollection.updateOne(eq(RECORD_ID, record.getRecordID()), set(DUPLICATES, record.getDuplicates()));
//      cnt += 1;
//    }
//  }
}
