package full_mode;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;
import records.Record;
import union.MergeType;
import union.Union;
import util.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.not;
import static util.Constants.*;
import static util.StringUtil.*;

public class FullMode {

  private final MongoClient mongoClient;
  private final String[] collections = {BGB_RECORDS, GBNS_RECORDS, BS_RECORDS, BMB_RECORDS};
  private MongoCollection<Record> unionCollection;
  private MongoCollection<Record> bgbCollection;
  private MongoCollection<Record> gbnsCollection;
  private MongoCollection<Record> bsCollection;
  private MongoCollection<Record> bmbCollection;

  public FullMode(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  public void start() {
    MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);

    unionCollection = database.getCollection(UNION_RECORDS, Record.class);
//    MongoCollection<TempRecord> tempRecordCollection = database.getCollection("tempRecord", TempRecord.class);

    bgbCollection = database.getCollection(BGB_RECORDS, Record.class);
    gbnsCollection = database.getCollection(GBNS_RECORDS, Record.class);
    bsCollection = database.getCollection(BS_RECORDS, Record.class);
    bmbCollection = database.getCollection(BMB_RECORDS, Record.class);

    // isbn merge
    Bson queryISBN = elemMatch(FIELDS, and(eq(NAME, _010), elemMatch(SUBFIELDS, and(eq(NAME, _a)))));   // get all which have isbn
    merge(MergeType.ISBN, queryISBN);

    // issn merge
    Bson queryISSN = and(not(elemMatch(FIELDS, and(eq(NAME, _010), elemMatch(SUBFIELDS, and(eq(NAME, _a)))))),
            elemMatch(FIELDS, and(eq(NAME, _011), elemMatch(SUBFIELDS, and(eq(NAME, _a))))));   // get all which have issn, but not isbn
    merge(MergeType.ISSN, queryISSN);

  }

  private void merge(MergeType mergeType, Bson query){
    Logger logger = new Logger(mergeType);

    MongoCursor<Record> bgbCursor = bgbCollection.find(query).cursor();
    MongoCursor<Record> gbnsCursor = gbnsCollection.find(query).cursor();
    //        MongoCursor<Record> bsCursor = bgbCollection.find(queryBS).cursor();
    //    MongoCursor<Record> bmbCursor = bgbCollection.find(query).cursor();

    long keysAddingStart = System.currentTimeMillis();
    logger.newLine();
    logger.info("Adding keys, inserting bgb records -> START");
    Map<String, Integer> bgbRecordKeys = new HashMap<>();  // (mergeType, recordID)  // TODO use redis instead of that

    // inserts in union bgb records and remember their MergeType
    int bgbCnt = 0;
    while (bgbCursor.hasNext()) {


      Record bgbRecord = bgbCursor.next();
      bgbRecord.setCameFrom(BGB);
      bgbRecord.setDuplicates(new ArrayList<>());
      bgbRecordKeys.put(removeDashes(getMergeTypeValue(bgbRecord, mergeType)), bgbRecord.getRecordID());

      unionCollection.insertOne(bgbRecord);
      bgbCnt += 1;
      if (bgbCnt >= 1000) {    // insert first 1000 bgb records
        break;
      }
    }
    long keysAddingEnd = System.currentTimeMillis();
    long keysAddingTimeElapsed = keysAddingEnd - keysAddingStart;
    logger.info("Adding keys, inserting bgb records -> END -> Time: " + keysAddingTimeElapsed);

    long mergeStart = System.currentTimeMillis();
    logger.info("Merging records -> START");
    int updateCnt = 0;
    int newRecordsCnt = 0;
    while (gbnsCursor.hasNext()) {
      Record gbnsRecord = gbnsCursor.next();

      Integer recordIdBgb = bgbRecordKeys.get(removeDashes(getMergeTypeValue(gbnsRecord, mergeType)));

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
        logger.err("NOT UNIQUE !! Record id: " + recordIdBgb);
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
    logger.info("Merging records -> END -> Time: " + mergeTimeElapsed);
    logger.newLine();
    logger.info("Retrieved from BGB: " + bgbCollection.countDocuments(query));
    logger.info("Retrieved from GBNS: " + gbnsCollection.countDocuments(query));
    logger.info("Union new BGB records: " + bgbCnt);
    logger.info("Union new GBNS records: " + newRecordsCnt);
    logger.info("Union update: " + updateCnt);
    logger.info("Union total: " + unionCollection.countDocuments());
    logger.separator();
  }

  private String getMergeTypeValue(Record record, MergeType mergeType) {
    if (mergeType == MergeType.ISBN) {
      return record.getISBN();
    } else {
      return record.getISSN();
    }
  }

  private void mergeByTitle() {
    Map<String, Integer> bgbRecordKeysTitle = new HashMap<>();  // (title, recordID)
  }
}
