/**
 * apriori from optimization Lk to Lk+1
 *
 * @author - Nikhil Yadav
 */
package AprioriGen;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;

public class AprioriGenOpt {

    public boolean DocumentChecker(Document d1, Document d2) {
        int size = d1.size();
        int i = 0;
        while (i < size - 1) {
            String key = "pos_" + i;

            int a=d1.getInteger(key);
            int b=d2.getInteger(key);
            if (a!=b){
                return false;
            }
            i++;
        }
        String key = "pos_" + (size - 1);
        int a=d1.getInteger(key);
        int b=d2.getInteger(key);
        if (a<b) {
            System.out.println("Adding "+d2.getInteger(key));

            return true;
        }
        return false;

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


    public ArrayList<Integer> intersection(ArrayList<Integer> a, ArrayList<Integer> b){
        ArrayList<Integer> ans=new ArrayList<>();
        for(int i :a){
            if(b.contains(i)){
                ans.add(i);
            }
        }
        Collections.sort(ans);
        return ans;
    }


    public boolean pruneCheck(Document d, FindIterable<Document> iter) {
        int size = d.size();
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String key = "pos_" + i;
            list.add(d.getInteger(key));
        }

        if(size>=2) {
            Set<Set<Integer>> string = Sets.combinations(ImmutableSet.copyOf(list), size - 1);
            for (Set<Integer> set : string) {
                System.out.println(set);
                List<String> l = new ArrayList(set);
                boolean check=false;
                Collections.sort(l);
                MongoCursor<Document> cursor = iter.iterator();
                while(cursor.hasNext()){
                    Document o1=cursor.next();
                    Document d1= (Document) o1.get("_id");
                    HashSet<Integer> hs=new HashSet<>();
                    for(int i=0;i<size;i++){
                        String key="pos_"+i;
                        hs.add(d1.getInteger(key));
                    }
                    for(int j=0;j<l.size();j++){
                        if(!hs.contains(l.get(j)))break;
                        if(j==l.size()-1)check=true;
                    }

                }
                if(!check)return false;

            }return true;
        }
        return false;
    }


    public void filterMinSup(MongoDatabase db, String collection, String ck,int minSup) {
        ArrayList<Integer> l = new ArrayList<>();
        FindIterable<Document> iter = db.getCollection(collection).find();
        int size = 0;
        for (Document doc : iter) {
            Document d = (Document) doc.get("_id");
            size = d.size();
            break;
        }
        iter = db.getCollection(collection).find();
        MongoCursor<Document> cursor = iter.iterator();

        int count=0;
        try {
            while (cursor.hasNext()) {
                Document o1 = cursor.next();
                Document d1 = (Document) o1.get("_id");
                int index1=o1.getInteger("order");

                BasicDBObject gtQuery = new BasicDBObject();
                gtQuery.put("order", new BasicDBObject("$gt", index1));

                MongoCursor<Document> cursor2 = db.getCollection(collection).find(gtQuery).iterator();

                while (cursor2.hasNext()) {
                    Document o2 = cursor2.next();
                    Document d2 = (Document) o2.get("_id");
                    int index2=o2.getInteger("order");

                    if (DocumentChecker(d1, d2)) {
                        String val = "pos_" + (size);
                        String get = "pos_" + (size - 1);
                        Document sub=copy(d1);
                        sub.append(val, d2.getInteger(get));
                        System.out.println(sub);
                        if(size>1) {
                            if (pruneCheck(sub, iter)) {
                                ArrayList<Integer> a= (ArrayList<Integer>) o1.get("transactions");
                                ArrayList<Integer> b= (ArrayList<Integer>) o2.get("transactions");
                                a=intersection(a,b);
                                if(a.size()>=minSup) {
                                    Document movie = new Document();
                                    movie.append("_id", sub);
                                    movie.append("order", count++);
                                    movie.append("transactions",a);
                                    System.out.println("prune check successfull, congrats " + movie);
                                    db.getCollection(ck).insertOne(movie);
                                }
                            }
                        }else{
                            ArrayList<Integer> a= (ArrayList<Integer>) o1.get("transactions");
                            ArrayList<Integer> b= (ArrayList<Integer>) o2.get("transactions");
                            a=intersection(a,b);
                            if(a.size()>=minSup) {
                                Document movie = new Document();
                                movie.append("_id", sub);
                                movie.append("order", count++);
                                movie.append("transactions",a);
                                System.out.println("prune check successfull, congrats " + movie);
                                db.getCollection(ck).insertOne(movie);
                            }
                        }


                    }else{
                        break;
                    }


                }


                //System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
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
        String mongoURL = args[0];
        String mongoDB = args[1];
        String collection = args[2];
        String newCollection = args[3];
        int minSup=Integer.parseInt(args[4]);

        MongoClient mongoClient = getClient(mongoURL);
        MongoDatabase db = mongoClient.getDatabase(mongoDB);
        AprioriGenOpt ago=new AprioriGenOpt();
        ago.filterMinSup(db,collection,newCollection,minSup);

    }
}
