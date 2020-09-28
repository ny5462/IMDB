package InitializePoints;
/**
 * program to fill the dimensions from SQL query to
 * mongo collection
 * @author- Nikhil Yadav
 */

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class InitializePoints {
    static Connection conn;

    public void fillCollection(MongoDatabase db, String collection,String query) throws SQLException {
        db.createCollection(collection);
        System.out.println("collection created");

        Statement st=conn.createStatement();
        ResultSet rs;

        rs=st.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();

        while(rs.next()){
            int id=rs.getInt("id");
            Document movie=new Document();
            movie.append("_id",id);
            List<Double> list=new ArrayList<>();
            for(int i=2;i<=columnsNumber;i++){
                String key="dim_"+i;
                System.out.println(key+":"+rs.getDouble(i));
                list.add(rs.getDouble(i));
            }
            movie.append("point",list);
            db.getCollection(collection).insertOne(movie);

        }

    }

    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }

    public static void main(String[] args)throws SQLException {
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
        InitializePoints ip=new InitializePoints();
        ip.fillCollection(db,collection,sql);
    }
}
