package union;

import com.mongodb.client.MongoCollection;
import lombok.Data;
import records.Record;

import java.util.HashMap;
import java.util.Map;

@Data
public class UnionDB {

  private static UnionDB instance;
  private int unionCurrentRecordId;
  private MongoCollection<Record> unionCollection;
  private Map<String, Integer> unionRecordKeys = new HashMap<>();
  private Map<String, Integer> currentRecordKeys = new HashMap<>();

  private UnionDB() {}

  public static UnionDB instance() {
    if(instance == null){
      instance = new UnionDB();
    }
    return instance;
  }

  public void insertToUnionCollection(Record record) {
    record.setRecordID(unionCurrentRecordId);
    unionCollection.insertOne(record);
    unionCurrentRecordId += 1;
  }

  public void setCurrentRecordId(Record record) {
    record.setRecordID(unionCurrentRecordId);
    unionCurrentRecordId += 1;
  }

  public void mergeRecordKeys() {
    unionRecordKeys.putAll(currentRecordKeys);
  }
}
