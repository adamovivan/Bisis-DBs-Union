import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import merge.MergeMode;
import merge.full_mode.FullMode;
import merge.incremental_mode.IncrementalMode;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import redis.clients.jedis.Jedis;
import util.Constants;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

class Main {

  private static MergeMode parseArgs(String[] args) {
    if (args.length < 1) {
      throw new IllegalArgumentException(
          "Missing merge mode argument (first argument). Merge mode can be either 'full' or 'incremental'");
    } else if (!args[0].toLowerCase().equals(MergeMode.FULL.name().toLowerCase()) && !args[0].toLowerCase()
                                                                                             .equals(
                                                                                                 MergeMode.INCREMENTAL.name()
                                                                                                                      .toLowerCase())) {
      throw new IllegalArgumentException("Wrong first argument. Merge mode can be either 'full' or 'incremental'");
    } else if (args[0].toLowerCase().equals(MergeMode.FULL.name().toLowerCase())) {
      return MergeMode.FULL;
    } else {
      return MergeMode.INCREMENTAL;
    }
  }

  public static void main(String[] args) {
    MergeMode mergeMode = parseArgs(args);

    ConnectionString connectionString = new ConnectionString(Constants.MONGO_CONNECTION_URL);
    Jedis jedis = new Jedis(Constants.REDIS_HOST, Constants.REDIS_PORT, Constants.REDIS_TIMEOUT);

    CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
    CodecRegistry codecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), pojoCodecRegistry);
    MongoClientSettings settings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .codecRegistry(codecRegistry)
            .build();

    MongoClient mongoClient = MongoClients.create(settings);

    switch (mergeMode) {
      case FULL:
        FullMode fullMode = new FullMode(mongoClient, jedis);
        fullMode.start();
        break;
      case INCREMENTAL:
        IncrementalMode incrementalMode = new IncrementalMode(mongoClient);
        incrementalMode.start();
        break;
    }
  }
}
