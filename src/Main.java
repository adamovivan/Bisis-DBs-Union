import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;


class Main {
    public static void main(String[] args) {

        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase("bisis");
        MongoCollection<Document> collection = database.getCollection("bgb_records");

        Iterable<Document> documents = collection.find();

        System.out.println(documents.iterator().next().toJson(JsonWriterSettings.builder().indent(true).build()));
    }
}
