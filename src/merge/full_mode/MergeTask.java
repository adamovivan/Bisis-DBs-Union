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
    System.out.println("Starting " + threadName);
    if (thread == null) {
      thread = new Thread(this, threadName);
      thread.start();
    }
  }

  public void run() {

    Logger logger = new Logger(mergeType);

    MongoCursor<Record> dbToMergeCursor = dbToMergeCollection.find(query).cursor();

    long mergeStart = System.currentTimeMillis();
    logger.info("Merging records -> START");
    int updateCnt = 0;
    int newRecordsCnt = 0;

    while (dbToMergeCursor.hasNext()) {
      Record dbToMergeRecord = dbToMergeCursor.next();

      Integer recordId = UnionDB.instance().getRecordKeys().get(removeDashes(RecordUtil.getMergeTypeValue(dbToMergeRecord, mergeType)));

      if (recordId == null) {
        // exists in dbToMerge, but not in union

        dbToMergeRecord.setCameFrom(dbToMerge);
        Union.setDefaultMetadata(dbToMergeRecord);
        UnionDB.instance().getRecordKeys().put(removeDashes(RecordUtil.getMergeTypeValue(dbToMergeRecord, mergeType)), UnionDB.instance().getUnionCurrentRecordId());

//        unionCollection.insertOne(dbToMergeRecord);
        synchronized(UnionDB.instance()) {
          UnionDB.instance().insertToUnionCollection(dbToMergeRecord);
        }

        newRecordsCnt += 1;
        continue;
      }

      Bson queryUnionRecord = eq(RECORD_ID, recordId);
      MongoCursor<Record> unionRecordsCursor = unionCollection.find(queryUnionRecord).cursor();

      if (unionCollection.countDocuments(queryUnionRecord) > 1) {
        logger.err("NOT UNIQUE !! Record id: " + recordId);
        //break;
      } else {
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
    logger.info("Retrieved from [" + dbToMerge.toUpperCase() + "]: " + dbToMergeCollection.countDocuments(query));
    //    logger.info("Union new BGB records: " + bgbCnt);
    logger.info("Union new [" + dbToMerge.toUpperCase() + "] records: " + newRecordsCnt);
    logger.info("Union update: " + updateCnt);
    logger.info("Union total: " + unionCollection.countDocuments());
    logger.separator();
  }


}
