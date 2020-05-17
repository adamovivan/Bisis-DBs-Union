package full_mode;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;
import records.Record;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static util.Constants.*;

public class FullMode {

    private final MongoClient mongoClient;
    private final String[] collections = {
            BGB_RECORDS,
            GBNS_RECORDS,
            BS_RECORDS,
            BMB_RECORDS
    };

    public FullMode(MongoClient mongoClient){
        this.mongoClient = mongoClient;
    }

    public void start(){
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);

        MongoCollection<Record> bgbCollection = database.getCollection(BGB_RECORDS, Record.class);
        MongoCollection<Record> gbnsCollection = database.getCollection(GBNS_RECORDS, Record.class);

        Bson query = elemMatch(FIELDS, and(eq(NAME, _010), elemMatch(SUBFIELDS, and(eq(NAME, _a), eq(CONTENT, "86-13-00157-2")))));

        MongoCursor<Record> bgbCursor = bgbCollection.find(query).cursor();
        System.out.println(bgbCollection.countDocuments(query));
        Record record1 = bgbCursor.next();

        System.out.println(record1);

    }
}
