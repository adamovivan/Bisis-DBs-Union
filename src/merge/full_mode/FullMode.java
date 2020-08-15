package merge.full_mode;

import com.google.gson.Gson;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;
import records.Record;
import redis.clients.jedis.Jedis;
import union.MergeType;
import union.Union;
import util.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.not;
import static util.Constants.*;
import static util.StringUtil.*;

public class FullMode {

  private static final int BATCH_SIZE = 1000;
  private final MongoClient mongoClient;
  private final Jedis redisClient;
  private final Gson gson;
  private final String[] dbsToMerge = {GBNS, BS, BMB};
  private MongoCollection<Record> unionCollection;
  private MongoCollection<Record> bgbCollection;
  private MongoCollection<Record> gbnsCollection;
  private MongoCollection<Record> bsCollection;
  private MongoCollection<Record> bmbCollection;
  private Map<String, Integer> recordKeys;
  private int unionCurrentRecordId;

  public FullMode(MongoClient mongoClient, Jedis redisClient) {
    this.mongoClient = mongoClient;
    this.redisClient = redisClient;
    this.gson = new Gson();
  }

  public void start() {
    MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);

    unionCollection = database.getCollection(UNION_RECORDS, Record.class);
    bgbCollection = database.getCollection(BGB_RECORDS, Record.class);
    gbnsCollection = database.getCollection(GBNS_RECORDS, Record.class);
    bsCollection = database.getCollection(BS_RECORDS, Record.class);
    bmbCollection = database.getCollection(BMB_RECORDS, Record.class);

    recordKeys = new HashMap<>();
    unionCurrentRecordId = 1;

    // isbn merge
    Bson queryISBN = elemMatch(FIELDS, and(eq(NAME, _010), elemMatch(SUBFIELDS, and(eq(NAME, _a)))));   // get all which have isbn
    merge(MergeType.ISBN, queryISBN);

//    // issn merge
//    Bson queryISSN = and(not(elemMatch(FIELDS, and(eq(NAME, _010), elemMatch(SUBFIELDS, and(eq(NAME, _a)))))),
//            elemMatch(FIELDS, and(eq(NAME, _011), elemMatch(SUBFIELDS, and(eq(NAME, _a))))));   // get all which have issn, but not isbn
//    merge(MergeType.ISSN, queryISSN);

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

    // inserts bgb records in union, remembers their MergeType, skips duplicates
    List<Record> batchRecords = new ArrayList<>();
    Set<String> bgbRecordKeys = new HashSet<>();
    int duplicates = 0;
    int bgbCnt = 0;
    while (bgbCursor.hasNext()) {
      Record bgbRecord = bgbCursor.next();
      String mergeKey = transformMergeKey(getMergeTypeValue(bgbRecord, mergeType), mergeType);

//      if (bgbRecordKeys.contains(mergeKey)) {
//        System.out.println(bgbRecord.getISBN());
//        duplicates += 1;
//        continue;
//      } else {
//        bgbRecordKeys.add(mergeKey);
//      }

      bgbRecord.setCameFrom(BGB);
      bgbRecord.setDuplicates(new ArrayList<>());
      addRecordToBatch(bgbRecord, batchRecords);
      recordKeys.put(transformMergeKey(getMergeTypeValue(bgbRecord, mergeType), mergeType), bgbRecord.getRecordID());

      if (batchRecords.size() >= BATCH_SIZE) {
        insertToUnionCollection(batchRecords);
      }

      bgbCnt += 1;
//      if (bgbCnt >= 1000) {    // insert first 1000 bgb records
//        break;
//      }
    }
    insertToUnionCollection(batchRecords);

    long keysAddingEnd = System.currentTimeMillis();
    long keysAddingTimeElapsed = keysAddingEnd - keysAddingStart;
    logger.info("Adding keys, inserting bgb records -> END -> Time: " + keysAddingTimeElapsed);
    logger.info("Retrieved from BGB: " + bgbCollection.countDocuments(query));
    logger.info("Union new BGB records: " + bgbCnt);

    logger.info("BGB keys: " + recordKeys.size());
    logger.info("BGB duplicates: " + duplicates);
//    for (String database: dbsToMerge) {
//      mergeWithUnionDatabase(database, mergeType, getCollectionByDatabaseName(database), query);
//    }
    mergeWithUnionDatabase(GBNS, mergeType, gbnsCollection, query);

    redisClient.flushAll();
  }

  private void mergeWithUnionDatabase(String dbToMerge, MergeType mergeType, MongoCollection<Record> dbToMergeCollection, Bson query) {
    Logger logger = new Logger(mergeType);

    MongoCursor<Record> dbToMergeCursor = dbToMergeCollection.find(query).cursor();
    logger.info("Retrieved from [" + dbToMerge.toUpperCase() + "]: " + gbnsCollection.countDocuments(query));

    long totalRecordIsNull = 0;
    long totalRecordIsNotNull = 0;
    long totalDocumentsCounting = 0;
    long totalInserting = 0;
    long totalRemoving = 0;
    long totalUpdating = 0;

    long totalNew1000 = 0;

    long mergeStart = System.currentTimeMillis();
    logger.info("Merging records -> START");
    int updateCnt = 0;
    int newRecordsCnt = 0;
    List<Record> batchRecords = new ArrayList<>();
    List<Record> recordsToUpdate = new ArrayList<>();
    List<Integer> idsToRemove = new ArrayList<>();
    Set<String> dbToMergeKeys = new HashSet<>();
    int duplicates = 0;

    while (dbToMergeCursor.hasNext()) {

      Record dbToMergeRecord = dbToMergeCursor.next();
      String mergeKey = transformMergeKey(getMergeTypeValue(dbToMergeRecord, mergeType), mergeType);

      if (dbToMergeKeys.contains(mergeKey)) {
//        System.out.println(dbToMergeRecord.getISBN());
        duplicates += 1;
        continue;
      } else {
        dbToMergeKeys.add(mergeKey);
      }

      Integer recordId = recordKeys.get(mergeKey);

      if (recordId == null) {
        // exists in dbToMerge, but not in union
        long startRecordIsNull = System.currentTimeMillis();
        dbToMergeRecord.setCameFrom(dbToMerge);
        Union.setDefaultMetadata(dbToMergeRecord);
        addRecordToBatch(dbToMergeRecord, batchRecords);
        //        recordKeys.put(removeDashes(getMergeTypeValue(dbToMergeRecord, mergeType)), dbToMergeRecord.getRecordID()); TODO merge keys

        if (batchRecords.size() >= BATCH_SIZE) {
          long startInserting = System.currentTimeMillis();
          insertToUnionCollection(batchRecords);
          totalInserting += System.currentTimeMillis() - startInserting;

//          logger.info("------------");
//          logger.info("Inserting new records");
//          logger.info("Total record is null: " + totalRecordIsNull);
//          logger.info("Total record is not null: " + totalRecordIsNotNull);
//          logger.info("Total union querying: " + totalUnionQuerying);
//          logger.info("Total documents counting: " + totalDocumentsCounting);
//          logger.info("Total inserting: " + totalInserting);
//          logger.info("Total removing: " + totalRemoving);
//          logger.info("Total updating: " + totalUpdating);
//          logger.info("Duplicates (skipped): " + duplicates);
//          logger.info("------------");
        }

        newRecordsCnt += 1;
        totalRecordIsNull += System.currentTimeMillis() - startRecordIsNull;
        continue;
      }
      Record unionRecord = gson.fromJson(redisClient.get(String.valueOf(recordId)), Record.class);

      long startRecordIsNotNull = System.currentTimeMillis();
      // exists in both dbToMerge and union
      Union.mergeRecords(unionRecord, dbToMergeRecord);
      Union.setDefaultMetadata(unionRecord);
      Union.addDuplicate(unionRecord, dbToMerge, dbToMergeRecord.getRn());

      recordsToUpdate.add(unionRecord);
      idsToRemove.add(unionRecord.getRecordID());

      if (recordsToUpdate.size() >= 1000) {
        long startRemoving = System.currentTimeMillis();
        unionCollection.deleteMany(in(RECORD_ID, idsToRemove));
          totalRemoving += System.currentTimeMillis() - startRemoving;
          long startInserting = System.currentTimeMillis();
          unionCollection.insertMany(recordsToUpdate);
          totalUpdating += System.currentTimeMillis() - startRemoving;
          totalInserting += System.currentTimeMillis() - startInserting;
          idsToRemove.clear();
          recordsToUpdate.clear();

//          logger.info("------------");
//          logger.info("Inserting updates");
//          logger.info("Total record is null: " + totalRecordIsNull);
//          logger.info("Total record is not null: " + totalRecordIsNotNull);
//          logger.info("Total documents counting: " + totalDocumentsCounting);
//          logger.info("Total inserting: " + totalInserting);
//          logger.info("Total removing: " + totalRemoving);
//          logger.info("Total updating: " + totalUpdating);
//          logger.info("Duplicates (skipped): " + duplicates);
//          logger.info("------------");
        }

        updateCnt += 1;
        totalRecordIsNotNull += System.currentTimeMillis() - startRecordIsNotNull;
    }
    insertToUnionCollection(batchRecords);

    long mergeEnd = System.currentTimeMillis();
    long mergeTimeElapsed = mergeEnd - mergeStart;
    logger.info("Merging records -> END -> Time: " + mergeTimeElapsed);
    logger.newLine();
    //    logger.info("Retrieved from BGB: " + bgbCollection.countDocuments(query));
    logger.info("Retrieved from [" + dbToMerge.toUpperCase() + "]: " + gbnsCollection.countDocuments(query));
    logger.info("Duplicates (skipped): " + duplicates);
    //    logger.info("Union new BGB records: " + bgbCnt);
    logger.info("Union new [" + dbToMerge.toUpperCase() + "] records: " + newRecordsCnt);
    logger.info("Union update: " + updateCnt);
    logger.info("Union total: " + unionCollection.countDocuments());
    logger.newLine();
    logger.info("Total record is null: " + totalRecordIsNull);
    logger.info("Total record is not null: " + totalRecordIsNotNull);
    logger.info("Total documents counting: " + totalDocumentsCounting);
    logger.separator();
  }

  private void addRecordToBatch(Record record, List<Record> batch) {
    record.setRecordID(unionCurrentRecordId);
    batch.add(record);
    unionCurrentRecordId += 1;

    redisClient.set(String.valueOf(record.getRecordID()), gson.toJson(record));
  }

  private void insertToUnionCollection(List<Record> records) {
    if (records.isEmpty()) {
      return;
    }
    unionCollection.insertMany(records);
    records.clear();
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
