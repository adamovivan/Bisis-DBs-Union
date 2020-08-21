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
  private int totalDuplicates;
  private int totalUpdates;
  private long totalTime;

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
    totalDuplicates = 0;
    totalUpdates = 0;

    long startTotal = System.currentTimeMillis();

    // isbn merge
    merge(MergeType.ISBN, Queries.queryIsbn());

    // issn merge
    merge(MergeType.ISSN, Queries.queryIssnNotIsbn());

    // title merge
    merge(MergeType.TITLE, Queries.queryTitleNotIsbnNotIssn());

    totalTime = System.currentTimeMillis() - startTotal;
    printResults();
    flushRedis();
  }

  private void merge(MergeType mergeType, Bson query) {
    Logger logger = new Logger(BGB, mergeType);

    MongoCursor<Record> bgbCursor = bgbCollection.find(query).cursor();

    long keysAddingStart = System.currentTimeMillis();
    logger.newLine();
    logger.info("Adding keys, inserting bgb records -> START");

    // inserts bgb records in union, remembers their MergeType, skips duplicates
    List<Record> batchRecords = new ArrayList<>();
    Set<String> bgbRecordKeys = new HashSet<>();
    int bgbDuplicates = 0;
    int bgbCnt = 0;
    while (bgbCursor.hasNext()) {
      Record bgbRecord = bgbCursor.next();
      String mergeKey = RecordUtil.createRecordKey(bgbRecord);

      if (bgbRecordKeys.contains(mergeKey)) {
        bgbDuplicates += 1;
        continue;
      } else {
        bgbRecordKeys.add(mergeKey);
      }

      bgbRecord.setMergeKey(mergeKey);
      bgbRecord.setCameFrom(BGB);
      bgbRecord.setDuplicates(new ArrayList<>());
      bgbRecord.setOriginRecordID(bgbRecord.getRecordID());
      Union.setDefaultMetadata(bgbRecord);
      addRecordToBatch(bgbRecord, batchRecords);
      recordKeys.put(mergeKey, bgbRecord.getRecordID());

      if (batchRecords.size() >= BATCH_SIZE) {
        insertToUnionCollection(batchRecords);
      }

      bgbCnt += 1;
    }
    insertToUnionCollection(batchRecords);

    totalDuplicates += bgbDuplicates;

    long keysAddingEnd = System.currentTimeMillis();
    long keysAddingTimeElapsed = keysAddingEnd - keysAddingStart;
    logger.info("Adding keys, inserting bgb records -> END -> Time: " + keysAddingTimeElapsed);
    logger.newLine();
    logger.info("Retrieved from BGB: " + bgbCollection.countDocuments(query));
    logger.info("BGB duplicates: " + bgbDuplicates);
    logger.info("Union new BGB records: " + bgbCnt);
    logger.info("Union total: " + unionCollection.countDocuments());
    logger.separator();

    for (String database : dbsToMerge) {
      mergeWithUnionDatabase(database, mergeType, getCollectionByDatabaseName(database), query);
    }
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
    Map<String, Integer> dbToMergeKeysNewRecords = new HashMap<>();
    int duplicates = 0;
    while (dbToMergeCursor.hasNext()) {

      Record dbToMergeRecord = dbToMergeCursor.next();
      String mergeKey = RecordUtil.createRecordKey(dbToMergeRecord);

      if (dbToMergeKeys.contains(mergeKey)) {
        duplicates += 1;
        continue;
      } else {
        dbToMergeKeys.add(mergeKey);
      }

      Integer recordId = recordKeys.get(mergeKey);

      if (recordId == null) {
        // exists in dbToMerge, but not in union
        dbToMergeRecord.setMergeKey(mergeKey);
        dbToMergeRecord.setCameFrom(dbToMerge);
        Union.setDefaultMetadata(dbToMergeRecord);
        dbToMergeRecord.setOriginRecordID(dbToMergeRecord.getRecordID());
        addRecordToBatch(dbToMergeRecord, batchRecords);
        dbToMergeKeysNewRecords.put(mergeKey, dbToMergeRecord.getRecordID());

        if (batchRecords.size() >= BATCH_SIZE) {
          insertToUnionCollection(batchRecords);
        }

        newRecordsCnt += 1;
        continue;
      }

      Record unionRecord = gson.fromJson(redisClient.get(String.valueOf(recordId)), Record.class);

      // exists in both dbToMerge and union
      Union.mergeRecords(unionRecord, dbToMergeRecord);
      Union.setDefaultMetadata(unionRecord);
      Union.addDuplicate(unionRecord, dbToMerge, dbToMergeRecord.getRecordID());

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

    // merge keys
    recordKeys.putAll(dbToMergeKeysNewRecords);

    totalDuplicates += duplicates;
    totalUpdates += updateCnt;

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

  private void flushRedis() {
    try {
      redisClient.flushAll();
    } catch (JedisConnectionException ignored) {
    } finally {
      redisClient.close();
    }
  }

  private void printResults() {
    long bgbTotal = bgbCollection.countDocuments();
    long gbnsTotal = gbnsCollection.countDocuments();
    long bsTotal = bsCollection.countDocuments();
    long bmbTotal = bmbCollection.countDocuments();

    long bgbOthers = bgbCollection.countDocuments(Queries.queryNotIsbnNotIssnNotTitle());
    long gbnsOthers = gbnsCollection.countDocuments(Queries.queryNotIsbnNotIssnNotTitle());
    long bsOthers = bsCollection.countDocuments(Queries.queryNotIsbnNotIssnNotTitle());
    long bmbOthers = bmbCollection.countDocuments(Queries.queryNotIsbnNotIssnNotTitle());

    System.out.println("\nTotal time: " + totalTime + "ms\n");
    System.out.println("BGB total: " + bgbTotal);
    System.out.println("GBNS total: " + gbnsTotal);
    System.out.println("BS total: " + bsTotal);
    System.out.println("BMB total: " + bmbTotal);
    System.out.println("Total: " + (bgbTotal + gbnsTotal + bsTotal + bmbTotal));
    System.out.println();
    System.out.println("BGB others: " + bgbOthers);
    System.out.println("GBNS others: " + gbnsOthers);
    System.out.println("BS others: " + bsOthers);
    System.out.println("BMB others: " + bmbOthers);
    System.out.println("Total: " + (bgbOthers + gbnsOthers + bsOthers + bmbOthers));
    System.out.println();
    System.out.println("Total duplicates: " + totalDuplicates);
    System.out.println("Total updates: " + totalUpdates);
    System.out.println();
    System.out.println("Union total: " + unionCollection.countDocuments());
  }
}
