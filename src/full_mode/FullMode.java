package full_mode;

import com.mongodb.Cursor;
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
import java.util.NoSuchElementException;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.not;
import static util.Constants.*;
import static util.StringUtil.*;

public class FullMode {

  private final MongoClient mongoClient;
  private final String[] dbsToMerge = {GBNS, BS, BMB};
  private MongoCollection<Record> unionCollection;
  private MongoCollection<Record> bgbCollection;
  private MongoCollection<Record> gbnsCollection;
  private MongoCollection<Record> bsCollection;
  private MongoCollection<Record> bmbCollection;
  private Map<String, Integer> recordKeys;
  private int unionCurrentRecordId;

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

    recordKeys = new HashMap<>();  // (mergeType, recordID)  // TODO use redis instead of that?
    unionCurrentRecordId = 1;

    // isbn merge
    Bson queryISBN = elemMatch(FIELDS, and(eq(NAME, _010), elemMatch(SUBFIELDS, and(eq(NAME, _a)))));   // get all which have isbn
    merge(MergeType.ISBN, queryISBN);

    // issn merge
    Bson queryISSN = and(not(elemMatch(FIELDS, and(eq(NAME, _010), elemMatch(SUBFIELDS, and(eq(NAME, _a)))))),
            elemMatch(FIELDS, and(eq(NAME, _011), elemMatch(SUBFIELDS, and(eq(NAME, _a))))));   // get all which have issn, but not isbn
    merge(MergeType.ISSN, queryISSN);

  }

  private void merge(MergeType mergeType, Bson query) {
    Logger logger = new Logger(mergeType);

    MongoCursor<Record> bgbCursor = bgbCollection.find(query).cursor();
//    MongoCursor<Record> gbnsCursor = gbnsCollection.find(query).cursor();
    //        MongoCursor<Record> bsCursor = bgbCollection.find(queryBS).cursor();
    //    MongoCursor<Record> bmbCursor = bgbCollection.find(query).cursor();

    long keysAddingStart = System.currentTimeMillis();
    logger.newLine();
    logger.info("Adding keys, inserting bgb records -> START");

    // inserts records in union bgb and remembers their MergeType
    int bgbCnt = 0;
    while (bgbCursor.hasNext()) {

      Record bgbRecord = bgbCursor.next();
      bgbRecord.setCameFrom(BGB);
      bgbRecord.setDuplicates(new ArrayList<>());
      recordKeys.put(removeDashes(getMergeTypeValue(bgbRecord, mergeType)), unionCurrentRecordId);
      insertToUnionCollection(bgbRecord);

      bgbCnt += 1;
      if (bgbCnt >= 1000) {    // insert first 1000 bgb records
        break;
      }
    }
    long keysAddingEnd = System.currentTimeMillis();
    long keysAddingTimeElapsed = keysAddingEnd - keysAddingStart;
    logger.info("Adding keys, inserting bgb records -> END -> Time: " + keysAddingTimeElapsed);
    logger.info("Retrieved from BGB: " + bgbCollection.countDocuments(query));
    logger.info("Union new BGB records: " + bgbCnt);

    for (String database: dbsToMerge) {
      mergeDatabaseWithUnion(database, mergeType, getCollectionByDatabaseName(database), query);
    }
  }

  private void mergeDatabaseWithUnion(String dbToMerge, MergeType mergeType, MongoCollection<Record> dbToMergeCollection, Bson query) {
    Logger logger = new Logger(mergeType);

    MongoCursor<Record> dbToMergeCursor = dbToMergeCollection.find(query).cursor();

    long mergeStart = System.currentTimeMillis();
    logger.info("Merging records -> START");
    int updateCnt = 0;
    int newRecordsCnt = 0;
    while (dbToMergeCursor.hasNext()) {
      Record dbToMergeRecord = dbToMergeCursor.next();

      Integer recordId = recordKeys.get(removeDashes(getMergeTypeValue(dbToMergeRecord, mergeType)));

      if (recordId == null) {
        // exists in dbToMerge, but not in union

        dbToMergeRecord.setCameFrom(dbToMerge);
        Union.setDefaultMetadata(dbToMergeRecord);
        recordKeys.put(removeDashes(getMergeTypeValue(dbToMergeRecord, mergeType)), unionCurrentRecordId);
        insertToUnionCollection(dbToMergeRecord);

        newRecordsCnt += 1;
        continue;
      }

      Bson queryUnionRecord = eq(RECORD_ID, recordId);
      MongoCursor<Record> unionRecordsCursor = unionCollection.find(queryUnionRecord).cursor();

      if (unionCollection.countDocuments(queryUnionRecord) > 1) {
        logger.err("NOT UNIQUE !! Record id: " + recordId);
        //break;
      }
      else {

        // exists in both dbToMerge and union
        Record unionRecord = unionRecordsCursor.next();

        Union.mergeRecords(unionRecord, dbToMergeRecord);
        Union.setDefaultMetadata(unionRecord);
        Union.addDuplicate(unionRecord, dbToMerge, dbToMergeRecord.getRn());

        unionCollection.updateOne(eq(RECORD_ID, unionRecord.getRecordID()), Union.getUpdates(unionRecord));

        updateCnt += 1;
      }
    }

    long mergeEnd = System.currentTimeMillis();
    long mergeTimeElapsed = mergeEnd - mergeStart;
    logger.info("Merging records -> END -> Time: " + mergeTimeElapsed);
    logger.newLine();
//    logger.info("Retrieved from BGB: " + bgbCollection.countDocuments(query));
    logger.info("Retrieved from [" + dbToMerge.toUpperCase() + "]: " + gbnsCollection.countDocuments(query));
//    logger.info("Union new BGB records: " + bgbCnt);
    logger.info("Union new [" + dbToMerge.toUpperCase() + "] records: " + newRecordsCnt);
    logger.info("Union update: " + updateCnt);
    logger.info("Union total: " + unionCollection.countDocuments());
    logger.separator();
  }

  private void insertToUnionCollection(Record record) {
    record.setRecordID(unionCurrentRecordId);
    unionCollection.insertOne(record);
    unionCurrentRecordId += 1;
  }

  private String getMergeTypeValue(Record record, MergeType mergeType) {
    if (mergeType == MergeType.ISBN) {
      return record.getISBN();
    } else {
      return record.getISSN();
    }
  }

  private MongoCollection<Record> getCollectionByDatabaseName(String database) {
    switch (database) {
      case GBNS: return gbnsCollection;
      case BS: return bsCollection;
      case BMB: return bmbCollection;
      default: throw new NoSuchElementException();
    }
  }

  private void mergeByTitle() {
    Map<String, Integer> bgbRecordKeysTitle = new HashMap<>();  // (title, recordID)
  }
}
