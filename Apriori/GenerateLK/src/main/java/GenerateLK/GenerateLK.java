/**
 * @author -Nikhil Yadav
 *
 * program to make Lk from Ck
 */
package GenerateLK;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;

public class GenerateLK {

    //https://mycourses.rit.edu/d2l/le/content/817251/viewContent/6254218/View
    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }

    public int TransactionChecker(MongoDatabase db,String collection, Document d){
        int count =0;
        int size=d.size();
        ArrayList<Integer> ls=new ArrayList<>();
        for(int i=0;i<size;i++){
            ls.add(d.getInteger("pos_"+i));
        }

        FindIterable<Document> iter = db.getCollection(collection).find();
        MongoCursor<Document> cursor = iter.iterator();
        while(cursor.hasNext()){
            Document o1=cursor.next();
            ArrayList<Integer> set= (ArrayList<Integer>) o1.get("items");
            boolean check=true;
            for(int i:ls){
                if(!set.contains(i)){
                    check=false;
                    break;
                }
            }if(check)count++;

        }
        return count;
    }

    public Document copy(Document d1) {
        Document cop=new Document();
        int size=d1.size();
        for(int i=0;i<size;i++){
            String key="pos_"+i;
            int val=d1.getInteger(key);
            cop.append(key,val);
        }
        return cop;
    }


    public void fillLK(MongoDatabase db,String collection, String ck, String lk, int minSup){
        FindIterable<Document> iter=db.getCollection(ck).find();
        MongoCursor<Document> cursor = iter.iterator();
        while(cursor.hasNext()){
            Document o1=cursor.next();
            Document d1= (Document) o1.get("_id");
            int reps=TransactionChecker(db,collection,d1);
            if(reps>=minSup){
                Document sub=copy(d1);
                Document movie=new Document();
                movie.append("_id",sub);
                movie.append("count",reps);
                db.getCollection(lk).insertOne(movie);
            }
        }

    }

    public static void main(String[] args) {
        String mongoURL = args[0];
        String mongoDB = args[1];
        String collectionTrans = args[2];
        String ck = args[3];
        int minSUp=Integer.parseInt(args[5]);
        String lk=args[4];

        MongoClient mongoClient = getClient(mongoURL);
        MongoDatabase db = mongoClient.getDatabase(mongoDB);
        db.createCollection(lk);

        GenerateLK glk=new GenerateLK();
        glk.fillLK(db,collectionTrans,ck,lk,minSUp);

    }
}
