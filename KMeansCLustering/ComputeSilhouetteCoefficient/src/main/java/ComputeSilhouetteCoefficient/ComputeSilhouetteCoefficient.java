package ComputeSilhouetteCoefficient;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static com.mongodb.client.model.Filters.eq;

public class ComputeSilhouetteCoefficient {
    int k;
    String collection;
    HashMap<Integer, Double> abmap = new HashMap<>();

    public ComputeSilhouetteCoefficient(MongoDatabase db, String collection) {
        MongoCursor<Integer> al = db.getCollection(collection).distinct("label", Integer.class).iterator();
        int count = 0;
        while (al.hasNext()) {
            count++;
            al.next();
        }
        this.k = count;
        this.collection = collection;
        System.out.println("no of clusters " + k);
    }


    public double euclidean(ArrayList<Double> doc1, ArrayList<Double> b) {
        double distance = 0.0;
        for (int i = 0; i < doc1.size(); i++) {
            distance += Math.pow(Math.abs(doc1.get(i) - b.get(i)), 2);
        }
        return Math.sqrt(distance);
    }


    public void AofX(MongoDatabase db) {

        for (int i = 0; i < k; i++) {
            Bson filter = Filters.and(
                    Filters.eq("label", i),
                    Filters.eq("isCentroid", false)
            );
            FindIterable<Document> iter = db.getCollection(collection).find(filter).batchSize(10000);
            MongoCursor<Document> cursor = iter.iterator();
            double div = (double) db.getCollection(collection).countDocuments(filter);
            System.out.println(div);

            while (cursor.hasNext()) {
                Document o1 = cursor.next();
                int id1 = o1.getInteger("_id");
                ArrayList<Double> points = (ArrayList<Double>) o1.get("point");
                double distance = 0.0;

                FindIterable<Document> iter2 = db.getCollection(collection).find(filter).batchSize(10000);
                MongoCursor<Document> cursor2 = iter2.iterator();

                while (cursor2.hasNext()) {
                    Document o2 = cursor2.next();
                    ArrayList<Double> points2 = (ArrayList<Double>) o2.get("point");
                    double dist = euclidean(points, points2);
                    distance += dist;
                }

                double avg = 0.0;
                if (distance == 0.0) {
                  //  System.out.println("average distance or a(x) for "+id1+"is "+avg);
                    abmap.put(id1, 0.0);
                } else {
                    avg = distance / (div);
                  //    System.out.println("average distance or a(x) for "+id1+"is "+avg);
                    abmap.put(id1, avg);
                }
            }

        }

       
    }


    public double sum(ArrayList<Double> val) {
        double sum = 0.0;
        for (double i : val) {
            sum += i;
        }
        return sum;
    }



    public void BofX(MongoDatabase db) {
        System.out.println("calculating b of x");
        for (int i = 0; i < k; i++) {
            Bson filter = Filters.and(
                    Filters.eq("label", i),
                    Filters.eq("isCentroid", false)
            );
            FindIterable<Document> iter = db.getCollection(collection).find(filter).batchSize(10000);
            MongoCursor<Document> cursor = iter.iterator();



            while (cursor.hasNext()) {
                Document o1 = cursor.next();
                int id1 = o1.getInteger("_id");
                ArrayList<Double> points = (ArrayList<Double>) o1.get("point");
                HashMap<Integer, ArrayList<Double>> hmap = new HashMap<>();
                double minClusterValue = Double.MAX_VALUE;
                int closest=i;
                Bson filter2 = Filters.and(
                        Filters.ne("label", i),
                        Filters.eq("isCentroid", false)
                );

                FindIterable<Document> iter2 = db.getCollection(collection).find(filter2).batchSize(10000);

                MongoCursor<Document> cursor2 = iter2.iterator();
                while (cursor2.hasNext()) {
                    Document o2 = cursor2.next();
                    ArrayList<Double> points2 = (ArrayList<Double>) o2.get("point");
                    int label = o2.getInteger("label");
                    double dist = euclidean(points, points2);
                    if (!hmap.containsKey(label)) {
                        ArrayList<Double> l = new ArrayList<>();
                        l.add(dist);
                        hmap.put(label, l);
                    } else {
                        ArrayList<Double> l = hmap.get(label);
                        l.add(dist);
                        hmap.put(label, l);
                    }
                }
                for (int min : hmap.keySet()) {
                    double avg =sum(hmap.get(min))/hmap.get(min).size();
                    if(avg<minClusterValue){
                        minClusterValue=avg;
                        closest=min;
                    }
                }
              
                double a = abmap.get(id1);

                double S= (minClusterValue-a) / Math.max(minClusterValue,a);
                System.out.println(id1+"-"+"a:"+a+", b:"+minClusterValue+", S:"+S+", closest:"+closest);
                db.getCollection(collection).updateOne(eq("_id",id1), Updates.set("S",S));


            }
        }
    }
//
//    public void BofX(MongoDatabase db) {
//        System.out.println("calculating b of x");
//
//        for (int i = 0; i < k; i++) {
//            Bson filter = Filters.and(
//                    Filters.eq("label", i),
//                    Filters.eq("isCentroid", false)
//            );
//            FindIterable<Document> iter = db.getCollection(collection).find(filter).batchSize(10000);
//            MongoCursor<Document> cursor = iter.iterator();
//
//            while (cursor.hasNext()) {
//                Document o1 = cursor.next();
//                int id1 = o1.getInteger("_id");
//                ArrayList<Double> points = (ArrayList<Double>) o1.get("point");
//
//                int minCluster=i;
//                double minClusterValue=Integer.MAX_VALUE;
//
//                for (int j = 0; j < k; j++) {
//                    if (i == j) j++;
//                    Bson filter2 = Filters.and(
//                            Filters.ne("label", i),
//                            Filters.eq("isCentroid", false)
//                    );
//                    double distance = 0.0;
//
//                    FindIterable<Document> iter2 = db.getCollection(collection).find(filter2).batchSize(10000);
//                    MongoCursor<Document> cursor2 = iter2.iterator();
//                    int div = (int) db.getCollection(collection).countDocuments(filter2);
//                    while (cursor2.hasNext()) {
//                        Document o2 = cursor2.next();
//                        ArrayList<Double> points2 = (ArrayList<Double>) o2.get("point");
//                        double dist = euclidean(points, points2);
//                        distance += dist;
//
//                    }
//                    double avg = 0.0;
//                    avg = distance / div;
//                    if(avg<minClusterValue){
//                        minClusterValue=avg;
//                        minCluster=j;
//                    }
//
//                }
//
//                double a=abmap.get(id1);
//                double b=minClusterValue;
//                double S=(b-a)/Math.max(a,b);
//                System.out.println(id1+"-"+"a:"+a+", b:"+minClusterValue+", S:"+S+", closest:"+minCluster);
//                System.out.println(id1+":"+S);
//
//
//            }
//        }
////
//    }

    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }

    public static void main(String[] args) {
        String mongoURL = args[0];
        String mongoDB = args[1];
        String collections = args[2];

        // https://www.w3resource.com/mongodb/mongodb-java-connection.php
        MongoClient mongoClient = getClient(mongoURL);
        MongoDatabase db = mongoClient.getDatabase(mongoDB);

        ComputeSilhouetteCoefficient sc = new ComputeSilhouetteCoefficient(db, collections);
        sc.AofX(db);
        sc.BofX(db);
        ArrayList<Double> a=new ArrayList<>();
        a.add(1951.0);
        a.add(8.2);
        a.add(111.0);
        ArrayList<Double> b=new ArrayList<>();
        b.add(1954.0);
        b.add(7.1);
        b.add(113.0);
        System.out.println(sc.euclidean(a,b));

    }
}
