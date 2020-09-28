/**
 * Program to perform mapping of given minsupport fields and
 * create a new collection
 * @author - Nikhil Yadav
 */

package GenerateL1;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;

public class GenerateL1 {

    static HashMap<Integer,Integer> map=new HashMap<>();

    public void FillMap(MongoDatabase db,String collection){

//credits - https://kb.objectrocket.com/mongo-db/how-to-iterate-through-mongodb-query-results-through-a-function-using-java-388
        FindIterable<Document> iterable = db.getCollection(collection).find();
        MongoCursor<Document> cursor = iterable.iterator(); // (2)
        try {
            while(cursor.hasNext()) {
                Object o=  cursor.next();
                ArrayList<Integer> list= (ArrayList<Integer>) ((Document) o).get("items");
                for(int i:list) {
                    map.put(i,map.getOrDefault(i,0)+1);
                }
                //System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        for(int i:map.keySet()){
            System.out.println(i+":"+map.get(i));
        }
    }

    public void AddToCollection(MongoDatabase db,String newCollection, int minSup){
        db.createCollection(newCollection);

        for(int i:map.keySet()){
            if(map.get(i)>=minSup){
                String temp="pos_0";
                Document sub=new Document();
                sub.append(temp,i);
                Document movie =new Document();
                movie.append("_id",sub);
                movie.append("count",map.get(i));
                db.getCollection(newCollection).insertOne(movie);

            }
        }
    }

    //https://mycourses.rit.edu/d2l/le/content/817251/viewContent/6254218/View
    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }



    public static void main(String[] args) {
        String mongoURL=args[0];
        String mongoDB=args[1];
        String collection=args[2];
        String newCollection=args[3];
        int minSup=Integer.parseInt(args[4]);

        // https://www.w3resource.com/mongodb/mongodb-java-connection.php
        MongoClient mongoClient = getClient(mongoURL);
        MongoDatabase db = mongoClient.getDatabase(mongoDB);

       // db.createCollection(collection);
        GenerateL1 gl=new GenerateL1();
        gl.FillMap(db,collection);
        gl.AddToCollection(db,newCollection,minSup);
    }
}
