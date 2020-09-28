package GenerateL1;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.TreeMap;

public class GenerateL1Opt {

    static TreeMap<Integer, ArrayList<Integer>> tmap=new TreeMap<>();

    public void FillMap(MongoDatabase db,String collection){
        FindIterable<Document> iterable = db.getCollection(collection).find();
        MongoCursor<Document> cursor = iterable.iterator(); // (2)
        try {
            while(cursor.hasNext()) {
                Object o=  cursor.next();
                ArrayList<Integer> list= (ArrayList<Integer>) ((Document) o).get("items");
                int tid=((Document) o).getInteger("_id");
                for(int i:list) {
                    if(!tmap.containsKey(i)) {
                        tmap.put(i,new ArrayList<Integer>());
                    }
                    tmap.get(i).add(tid);
                }
                //System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        for(int i:tmap.keySet()){
            System.out.println(i+":"+tmap.get(i));
        }
    }

    public void AddToCollection(MongoDatabase db,String newCollection, int minSup){
        db.createCollection(newCollection);
        int count=0;
        for(int i:tmap.keySet()){
            if(tmap.get(i).size()>=minSup){
                String temp="pos_0";
                Document sub=new Document();
                sub.append(temp,i);
                Document movie =new Document();
                movie.append("_id",sub);
                movie.append("transactions",tmap.get(i));
                movie.append("order",count++);
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
        GenerateL1Opt gl=new GenerateL1Opt();
        gl.FillMap(db,collection);
        gl.AddToCollection(db,newCollection,minSup);

    }
}
