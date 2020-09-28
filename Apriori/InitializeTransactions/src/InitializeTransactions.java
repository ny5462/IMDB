/**
program that creates L1 to initialize apriori algorithm for association mining
@author - Nikhil Yadav
**/
package InitializeTransactions;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

public class InitializeTransactions {
    static Connection conn=null;


    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }

    public void FillCollection(String query,MongoDatabase db,String collection) throws SQLException {
        ResultSet rs;
        Statement st;

        st=conn.createStatement();
        rs=st.executeQuery(query);

        int prevTid=0;
        while(rs.next()){
            int tid=rs.getInt("tid");
            int iid=rs.getInt("iid");

            if(prevTid==0||prevTid!=tid){
                Document movie= new Document();
                List<Integer> l=new ArrayList<>();
                l.add(iid);
                movie.append("_id",tid);
                movie.append("items",l);
                db.getCollection(collection).insertOne(movie);

            }else{
                db.getCollection(collection).updateOne(eq("_id", tid), Updates.addToSet("items", iid));
            }
            prevTid=tid;


        }


    }

    public static void main(String[] args) throws SQLException {
        String dbURL=args[0];
        String user=args[1];
        String pwd=args[2];
        String sql=args[3];
        String mongoURL=args[4];
        String mongoDB=args[5];
        String collection=args[6];

        conn = DriverManager.getConnection(dbURL, user, pwd);


        // https://www.w3resource.com/mongodb/mongodb-java-connection.php
        MongoClient mongoClient = getClient(mongoURL);
        MongoDatabase db = mongoClient.getDatabase(mongoDB);

        db.createCollection(collection);
        InitializeTransactions it=new InitializeTransactions();
        it.FillCollection(sql,db,collection);
    }
}
