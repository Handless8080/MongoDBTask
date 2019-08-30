import org.junit.Test;

public class MongoDBTest {
    private MongoConnection mongoConnection = MongoConnection.getDbConnection();

    @Test
    public void analyticFunctionTest() {
        mongoConnection.countPagesVisitAnalyticFunction();
    }

    @Test
    public void mapReduceTest() {
        mongoConnection.countPagesVisitMapReduce();
    }

    @Test
    public void getMostPopularStudentsTest() {
        mongoConnection.getMostPopularStudents();
    }
}
