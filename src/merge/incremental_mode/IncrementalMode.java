package merge.incremental_mode;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import records.Duplicate;
import records.Record;
import union.MergeType;
import union.Queries;
import union.Union;
import util.Logger;
import util.RecordUtil;

import java.time.LocalDateTime;
import java.time.Period;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import static util.Constants.*;

public class IncrementalMode {

  private final MongoClient mongoClient;
  private final String[] dbsToMerge = {BGB, GBNS, BS, BMB};
  private MongoCollection<Record> unionCollection;
  private MongoCollection<Record> bgbCollection;
  private MongoCollection<Record> gbnsCollection;
  private MongoCollection<Record> bsCollection;
  private MongoCollection<Record> bmbCollection;
  private LocalDateTime lastUpdate;

  private int unionCurrentRecordId;
  private int totalDuplicates;
  private int totalUpdates;
  private long totalTime;

  public IncrementalMode(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  public void start() {
    MongoDatabase mongoDatabase = mongoClient.getDatabase(DATABASE_NAME);

    unionCollection = mongoDatabase.getCollection(UNION_RECORDS, Record.class);
    bgbCollection = mongoDatabase.getCollection(BGB_RECORDS, Record.class);
    gbnsCollection = mongoDatabase.getCollection(GBNS_RECORDS, Record.class);
    bsCollection = mongoDatabase.getCollection(BS_RECORDS, Record.class);
    bmbCollection = mongoDatabase.getCollection(BMB_RECORDS, Record.class);

    lastUpdate = LocalDateTime.now().minus(Period.ofDays(1));    // TODO change, (cron job?)

    unionCurrentRecordId = (int) unionCollection.countDocuments() + 1;

    totalDuplicates = 0;
    totalUpdates = 0;

    long startTotal = System.currentTimeMillis();

    totalTime = System.currentTimeMillis() - startTotal;

        for (String database : dbsToMerge) {
          mergeWithUnionDatabase(database, getCollectionByDatabaseName(database));
        }
//    mergeWithUnionDatabase(GBNS, gbnsCollection);
    printResults();
  }

  private void mergeWithUnionDatabase(String dbToMerge, MongoCollection<Record> dbToMergeCollection) {

    long unionTotalBefore = unionCollection.countDocuments();
    Bson query = Queries.queryCreationDate(lastUpdate.toString());
    Logger logger = new Logger(dbToMerge, MergeType.INCREMENTAL);
    long mergeStart = System.currentTimeMillis();
    logger.newLine();
    logger.info("Merging records -> START");
//    System.out.println(lastUpdate);
//    System.out.println(dbToMergeCollection.countDocuments(query));

    MongoCursor<Record> dbToMergeCursor = dbToMergeCollection.find(query).cursor();

    Set<String> dbToMergeKeys = new HashSet<>();

    int duplicates = 0;
    int updateCnt = 0;
    int newRecordsCnt = 0;
    while (dbToMergeCursor.hasNext()) {

      Record dbToMergeRecord = dbToMergeCursor.next();
      String mergeKey = RecordUtil.createRecordKey(dbToMergeRecord);

      if (dbToMergeKeys.contains(mergeKey)) {
        duplicates += 1;
        continue;
      } else {
        dbToMergeKeys.add(mergeKey);
      }

      MongoCursor<Record> unionCursor = unionCollection.find(Queries.queryMergeKey(mergeKey)).cursor();
//      System.out.println("MERGE KEY CNT: " + unionCollection.countDocuments(Queries.queryMergeKey(mergeKey)));
//      System.out.println("MK: " + mergeKey);

      if (!unionCursor.hasNext()) {
        // exists in dbToMerge, but not in union
        dbToMergeRecord.setMergeKey(mergeKey);
        dbToMergeRecord.setCameFrom(dbToMerge);
        Union.setDefaultMetadata(dbToMergeRecord);

        dbToMergeKeys.add(mergeKey);

        dbToMergeRecord.setOriginRecordID(dbToMergeRecord.getRecordID());
        dbToMergeRecord.setRecordID(unionCurrentRecordId);
        unionCollection.insertOne(dbToMergeRecord);

        newRecordsCnt += 1;
        continue;
      }

      // exists in both dbToMerge and union
      Record unionRecord = unionCursor.next();

      if (isAlreadyAdded(unionRecord, dbToMerge)) {
        duplicates += 1;
        continue;
      }

      Union.mergeRecords(unionRecord, dbToMergeRecord);
      Union.setUpdateMetadata(unionRecord);
      Union.updateDuplicates(unionRecord, dbToMerge, dbToMergeRecord.getRecordID());

      // update
      unionCollection.deleteOne(Filters.eq(RECORD_ID, unionRecord.getRecordID()));
      unionCollection.insertOne(unionRecord);

      updateCnt += 1;
    }

    totalDuplicates += duplicates;
    totalUpdates += updateCnt;

    long mergeEnd = System.currentTimeMillis();
    long mergeTimeElapsed = mergeEnd - mergeStart;
    logger.info("Merging records -> END -> Time: " + mergeTimeElapsed);
    logger.newLine();
    logger.info("Union total before: " + unionTotalBefore);
    logger.info("Retrieved from [" + dbToMerge.toUpperCase() + "]: " + dbToMergeCollection.countDocuments(query));
    logger.info("Duplicates (skipped): " + duplicates);
    logger.info("Union new [" + dbToMerge.toUpperCase() + "] records: " + newRecordsCnt);
    logger.info("Union update: " + updateCnt);
    logger.info("Union total: " + unionCollection.countDocuments());
    logger.separator();
  }

  private boolean isAlreadyAdded(Record unionRecord, String dbToMerge) {
    if (unionRecord.getCameFrom().equals(dbToMerge)) {
      return true;
    }

    if (unionRecord.getDuplicates() == null) {
      return false;
    }

    for (Duplicate duplicate: unionRecord.getDuplicates()) {
      if (duplicate.getName().equals(dbToMerge)) {
        return true;
      }
    }
    return false;
  }

  private MongoCollection<Record> getCollectionByDatabaseName(String database) {
    switch (database) {
      case BGB:
        return bgbCollection;
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

  private void printResults() {
    long bgbTotal = bgbCollection.countDocuments();
    long gbnsTotal = gbnsCollection.countDocuments();
    long bsTotal = bsCollection.countDocuments();
    long bmbTotal = bmbCollection.countDocuments();

    System.out.println("\nTotal time: " + totalTime + "ms\n");
    System.out.println("BGB total: " + bgbTotal);
    System.out.println("GBNS total: " + gbnsTotal);
    System.out.println("BS total: " + bsTotal);
    System.out.println("BMB total: " + bmbTotal);
    System.out.println("Total: " + (bgbTotal + gbnsTotal + bsTotal + bmbTotal));
    System.out.println();
    System.out.println("Total duplicates: " + totalDuplicates);
    System.out.println("Total updates: " + totalUpdates);
    System.out.println();
    System.out.println("Union total: " + unionCollection.countDocuments());
  }
}
