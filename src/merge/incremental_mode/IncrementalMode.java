package merge.incremental_mode;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import records.Record;
import union.Queries;

import java.util.HashMap;
import java.util.NoSuchElementException;

import static util.Constants.*;

public class IncrementalMode {

  private final MongoClient mongoClient;
  private final String[] dbsToMerge = {BGB, GBNS, BS, BMB};
  private MongoCollection<Record> unionCollection;
  private MongoCollection<Record> bgbCollection;
  private MongoCollection<Record> gbnsCollection;
  private MongoCollection<Record> bsCollection;
  private MongoCollection<Record> bmbCollection;

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

    unionCurrentRecordId = 1;
    totalDuplicates = 0;
    totalUpdates = 0;

    long startTotal = System.currentTimeMillis();

    totalTime = System.currentTimeMillis() - startTotal;

    for (String database : dbsToMerge) {
      mergeWithUnionDatabase(database, getCollectionByDatabaseName(database));
    }

    printResults();
  }

  private void mergeWithUnionDatabase(String database, MongoCollection<Record> dbToMergeCollection) {

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

//    long bgbOthers = bgbCollection.countDocuments(Queries.queryNotIsbnNotIssnNotTitle());
//    long gbnsOthers = gbnsCollection.countDocuments(Queries.queryNotIsbnNotIssnNotTitle());
//    long bsOthers = bsCollection.countDocuments(Queries.queryNotIsbnNotIssnNotTitle());
//    long bmbOthers = bmbCollection.countDocuments(Queries.queryNotIsbnNotIssnNotTitle());

    System.out.println("\nTotal time: " + totalTime + "ms\n");
    System.out.println("BGB total: " + bgbTotal);
    System.out.println("GBNS total: " + gbnsTotal);
    System.out.println("BS total: " + bsTotal);
    System.out.println("BMB total: " + bmbTotal);
    System.out.println("Total: " + (bgbTotal + gbnsTotal + bsTotal + bmbTotal));
    System.out.println();
//    System.out.println("BGB others: " + bgbOthers);
//    System.out.println("GBNS others: " + gbnsOthers);
//    System.out.println("BS others: " + bsOthers);
//    System.out.println("BMB others: " + bmbOthers);
//    System.out.println("Total: " + (bgbOthers + gbnsOthers + bsOthers + bmbOthers));
    System.out.println();
    System.out.println("Total duplicates: " + totalDuplicates);
    System.out.println("Total updates: " + totalUpdates);
    System.out.println();
    System.out.println("Union total: " + unionCollection.countDocuments());
  }
}
