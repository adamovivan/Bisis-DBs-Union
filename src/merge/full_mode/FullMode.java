package merge.full_mode;

import com.google.gson.Gson;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;
import records.Record;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import union.MergeType;
import union.Queries;
import union.Union;
import util.Logger;
import util.RecordUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.mongodb.client.model.Filters.in;
import static util.Constants.*;

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

    long startTotal = System.currentTimeMillis();

    // isbn merge
    merge(MergeType.ISBN, Queries.queryIsbn);

    // issn merge
    merge(MergeType.ISSN, Queries.queryIssnNotIsbn);

    // title merge
    merge(MergeType.TITLE, Queries.queryTitleNotIsbnNotIssn);

    System.out.println("Total time: " + (System.currentTimeMillis() - startTotal));

    flushRedis();
  }

  private void merge(MergeType mergeType, Bson query) {
    Logger logger = new Logger(BGB, mergeType);

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
      //      String mergeKey = transformMergeKey(getMergeTypeValue(bgbRecord, mergeType), mergeType);
      String mergeKey = RecordUtil.createRecordKey(bgbRecord);

      if (bgbRecordKeys.contains(mergeKey)) {
        //        System.out.println(mergeKey);
        duplicates += 1;
        continue;
      } else {
        bgbRecordKeys.add(mergeKey);
      }

      bgbRecord.setCameFrom(BGB);
      bgbRecord.setDuplicates(new ArrayList<>());
      addRecordToBatch(bgbRecord, batchRecords);
      recordKeys.put(mergeKey, bgbRecord.getRecordID());
      //      addRecordKey(bgbRecord, mergeType);
      //      recordKeys.put(transformMergeKey(getMergeTypeValue(bgbRecord, mergeType), mergeType), bgbRecord.getRecordID());

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
    logger.newLine();
    logger.info("Retrieved from BGB: " + bgbCollection.countDocuments(query));
    logger.info("Union new BGB records: " + bgbCnt);
    logger.info("BGB keys: " + recordKeys.size());
    logger.info("BGB duplicates: " + duplicates);
    logger.separator();

//    for (String database : dbsToMerge) {
//      mergeWithUnionDatabase(database, mergeType, getCollectionByDatabaseName(database), query);
//    }
    mergeWithUnionDatabase(GBNS, mergeType, gbnsCollection, query);
  }

  private void mergeWithUnionDatabase(String dbToMerge, MergeType mergeType,
      MongoCollection<Record> dbToMergeCollection, Bson query) {
    Logger logger = new Logger(dbToMerge, mergeType);

    MongoCursor<Record> dbToMergeCursor = dbToMergeCollection.find(query).cursor();

    long mergeStart = System.currentTimeMillis();
    logger.newLine();
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
      //      String mergeKey = transformMergeKey(getMergeTypeValue(dbToMergeRecord, mergeType), mergeType);
      String mergeKey = RecordUtil.createRecordKey(dbToMergeRecord);

      if (dbToMergeKeys.contains(mergeKey)) {
        //        System.out.println(dbToMergeRecord.getISBN());
        duplicates += 1;
        continue;
      } else {
        dbToMergeKeys.add(mergeKey);
      }

      //      Integer recordId = recordKeys.get(mergeKey);
      Integer recordId = recordKeys.get(mergeKey);

      if (recordId == null) {
        // exists in dbToMerge, but not in union
        dbToMergeRecord.setCameFrom(dbToMerge);
        Union.setDefaultMetadata(dbToMergeRecord);
        addRecordToBatch(dbToMergeRecord, batchRecords);
        //        recordKeys.put(removeDashes(getMergeTypeValue(dbToMergeRecord, mergeType)), dbToMergeRecord.getRecordID()); TODO merge keys

        if (batchRecords.size() >= BATCH_SIZE) {
          insertToUnionCollection(batchRecords);
        }

        newRecordsCnt += 1;
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

      // update redis
      redisClient.set(String.valueOf(unionRecord.getRecordID()), gson.toJson(unionRecord));

      if (recordsToUpdate.size() >= 1000) {
        updateUnionCollection(recordsToUpdate, idsToRemove);
      }

      updateCnt += 1;
    }
    insertToUnionCollection(batchRecords);
    updateUnionCollection(recordsToUpdate, idsToRemove);

    long mergeEnd = System.currentTimeMillis();
    long mergeTimeElapsed = mergeEnd - mergeStart;
    logger.info("Merging records -> END -> Time: " + mergeTimeElapsed);
    logger.newLine();
    logger.info("Retrieved from [" + dbToMerge.toUpperCase() + "]: " + dbToMergeCollection.countDocuments(query));
    logger.info("Duplicates (skipped): " + duplicates);
    logger.info("Union new [" + dbToMerge.toUpperCase() + "] records: " + newRecordsCnt);
    logger.info("Union update: " + updateCnt);
    logger.info("Union total: " + unionCollection.countDocuments());
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

  private void updateUnionCollection(List<Record> recordsToUpdate, List<Integer> idsToRemove) {
    if (recordsToUpdate.isEmpty()) {
      return;
    }

    unionCollection.deleteMany(in(RECORD_ID, idsToRemove));
    unionCollection.insertMany(recordsToUpdate);

    idsToRemove.clear();
    recordsToUpdate.clear();
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
      case GBNS:
        return gbnsCollection;
      case BS:
        return bsCollection;
      case BMB:
        return bmbCollection;
      default:
        throw new NoSuchElementException();
    }
  }

  private void mergeByTitle() {
    Map<String, Integer> bgbRecordKeysTitle = new HashMap<>();  // (title, recordID)
  }

  private void flushRedis() {
    try {
      redisClient.flushAll();
    } catch (JedisConnectionException ignored) {
    } finally {
      redisClient.close();
    }
  }
}
