package merge.full_mode;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import lombok.Data;
import org.bson.conversions.Bson;
import records.Record;
import union.MergeType;
import union.Union;
import union.UnionDB;
import util.Logger;
import util.RecordUtil;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static util.Constants.RECORD_ID;
import static util.StringUtil.removeDashes;

@Data
public class MergeTask extends Thread {
  private Thread thread;
  private String threadName;
  private MongoCollection<Record> dbToMergeCollection;
  private MongoCollection<Record> unionCollection;
  private int skip;
  private int limit;
  private String dbToMerge;
  private MergeType mergeType;
  private Bson query;

  public MergeTask(String name, int skip, int limit, String dbToMerge, MergeType mergeType, MongoCollection<Record> dbToMergeCollection, MongoCollection<Record> unionCollection, Bson query) {
    this.threadName = name;
    this.dbToMergeCollection = dbToMergeCollection;
    this.unionCollection = unionCollection;
    this.skip = skip;
    this.limit = limit;
    this.dbToMerge = dbToMerge;
    this.mergeType = mergeType;
    this.query = query;
  }

//  public void run() {
//    MongoCursor<Record> bgbCursor = dbToMergeCollection.find().skip(skip).limit(limit).cursor();
//    List<Record> records = new ArrayList<>();
//    while (bgbCursor.hasNext()) {
//      Record record = bgbCursor.next();
//      records.add(record);
//      if (records.size() > 1000) {
//        unionCollection.insertMany(records);
//        records.clear();
//      }
//    }
//    unionCollection.insertMany(records);
//  }

  public void start() {
    if (thread == null) {
      thread = new Thread(this, threadName);
      thread.start();
    }
  }

  public void run() {
    long sveStart = System.currentTimeMillis();
    Logger logger = new Logger(mergeType, threadName);
    System.out.println("Starting [" + threadName + "] Batch size: " + limit);

    MongoCursor<Record> dbToMergeCursor = dbToMergeCollection.find(query).skip(skip).limit(limit).cursor();
    List<Record> records = new ArrayList<>();

    long mergeStart = System.currentTimeMillis();
    logger.info("Merging records -> START");
    int updateCnt = 0;
    int newRecordsCnt = 0;

    long UKUPNOgetKeys = 0;
    long UKUPNOrecordIdNull = 0;
    long ukupnoNOTNULL = 0;
    long ukupnoQueryUnion = 0;

    while (dbToMergeCursor.hasNext()) {
      Record dbToMergeRecord = dbToMergeCursor.next();

      Integer recordId;

      long startGetKeys = System.currentTimeMillis();
      recordId = UnionDB.instance().getUnionRecordKeys().get(removeDashes(RecordUtil.getMergeTypeValue(dbToMergeRecord, mergeType)));

      UKUPNOgetKeys += System.currentTimeMillis() - startGetKeys;

      if (recordId == null) {
        // exists in dbToMerge, but not in union

        long startMERENJErecordIdNull = System.currentTimeMillis();
        dbToMergeRecord.setCameFrom(dbToMerge);
        Union.setDefaultMetadata(dbToMergeRecord);

        synchronized(UnionDB.instance()) {
          UnionDB.instance().getCurrentRecordKeys().put(removeDashes(RecordUtil.getMergeTypeValue(dbToMergeRecord, mergeType)), UnionDB.instance().getUnionCurrentRecordId());
          UnionDB.instance().setCurrentRecordId(dbToMergeRecord);
        }
//        unionCollection.insertOne(dbToMergeRecord);
        records.add(dbToMergeRecord);
        if (records.size() >= 100) {
          unionCollection.insertMany(records);
          records.clear();
        }
        UKUPNOrecordIdNull += System.currentTimeMillis() - startMERENJErecordIdNull;
        newRecordsCnt += 1;
        continue;
      }

      //////////############///////////////

      long queryUnionStart = System.currentTimeMillis();
      Bson queryUnionRecord = eq(RECORD_ID, recordId);
      MongoCursor<Record> unionRecordsCursor = unionCollection.find(queryUnionRecord).cursor();
      ukupnoQueryUnion += System.currentTimeMillis() - queryUnionStart;

      if (unionCollection.countDocuments(queryUnionRecord) > 1) {
        logger.err("NOT UNIQUE !! Record id: " + recordId);
        //break;
      } else {
        long notNullStart = System.currentTimeMillis();
        // exists in both dbToMerge and union
        Record unionRecord = unionRecordsCursor.next();

        Union.mergeRecords(unionRecord, dbToMergeRecord);
        Union.setDefaultMetadata(unionRecord);
        Union.addDuplicate(unionRecord, dbToMerge, dbToMergeRecord.getRn());

        unionCollection.updateOne(eq(RECORD_ID, unionRecord.getRecordID()), Union.getUpdates(unionRecord));

        updateCnt += 1;

        ukupnoNOTNULL += System.currentTimeMillis() - notNullStart;
      }
    }
    unionCollection.insertMany(records);

    long mergeEnd = System.currentTimeMillis();
    long mergeTimeElapsed = mergeEnd - mergeStart;
    logger.info("Merging records -> END -> Time: " + mergeTimeElapsed);
    logger.newLine();
    //    logger.info("Retrieved from BGB: " + bgbCollection.countDocuments(query));
    logger.info("Retrieved from [" + dbToMerge.toUpperCase() + "]: " + dbToMergeCollection.countDocuments(query));
    logger.info("Batch size [" + dbToMerge.toUpperCase() + "]: " + limit);
    //    logger.info("Union new BGB records: " + bgbCnt);
    logger.info("Union new [" + dbToMerge.toUpperCase() + "] records: " + newRecordsCnt);
    logger.info("Union update: " + updateCnt);
    logger.info("Union total: " + unionCollection.countDocuments());
    logger.info("UKUPNOgetKeys: " + UKUPNOgetKeys);
    logger.info("UKUPNOrecordIdNull: " + UKUPNOrecordIdNull);
    logger.info("UKUPNONOTnull: " + ukupnoNOTNULL);
    logger.info("UKUPNO Query Union: " + ukupnoQueryUnion);
    logger.info("SVE: " + (System.currentTimeMillis() - sveStart));
    Logger.separator();
  }


}
