import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class testCassandra {

    public static void main(String[] args) {
        String serverIP = "127.0.0.1";
        String keyspace = "bigData";

        Cluster cluster = Cluster.builder()
                .addContactPoints(serverIP)
                .build();

        Session session = cluster.connect(keyspace);

        String createTable = "CREATE TABLE IF NOT EXISTS reviews (review_id varchar Primary Key,marketplace varchar, verified_purchase varchar, helpful_votes bigint," +
                "product_parent varchar, review_date date, total_votes bigint, product_id varchar, star_rating bigint, customer_id varchar," +
                "vine varchar, product_category varchar);";

        session.execute(createTable);
    }

}

