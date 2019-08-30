import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.MapReduceAction;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.util.Collections;

class MongoConnection {
    private MongoDatabase db;
    private Block<Document> printBlock = document -> System.out.println(document.toJson());

    private MongoConnection(String host, int port, String dbName) {
        MongoClient mongo = new MongoClient(host, port);
        db = mongo.getDatabase(dbName);
    }

    static MongoConnection getDbConnection() {
        return new MongoConnection("localhost", 27017, "db");
    }

    void countPagesVisitAnalyticFunction() {
        MongoCollection<Document> collection = db.getCollection("hits");

        collection.aggregate(Collections.singletonList(
                Aggregates.group("$resource", Accumulators.sum("count", 1))
        )).forEach(printBlock);
    }

    void countPagesVisitMapReduce() {
        MongoCollection<Document> collection = db.getCollection("hits");

        String map = "function() {" +
                    "emit(this.resource, {});" +
                "}";

        String reduce = "function(key, values) {" +
                    "return values.length;" +
                "}";

        collection.mapReduce(map, reduce).forEach(printBlock);
    }

    void getMostPopularStudents() {
        MongoCollection<Document> collection = db.getCollection("students");

        String map = "function() {" +
                    "for (let i = 0; i < this.friends.length; i++) {" +
                        "if (this._id !== this.friends[i]) {" +
                            "emit(this._id, this.friends[i]);" +
                        "}" +
                        "emit(this.friends[i], this._id);" +
                    "}" +
                "}";

        String reduce = "function(key, values) {" +
                    "let uniqueIndexes = values.filter(function(item, index) {" +
                        "return values.indexOf(item) < index;" +
                    "});" +
                    "return uniqueIndexes.length;" +
                "}";

        collection.mapReduce(map, reduce).collectionName("friend_result").toCollection();
        db.getCollection("friend_result").find()
                .sort(Sorts.descending("value"))
                .limit(5)
                .forEach(printBlock);
    }
}
