package KMeans;
/**
 * @author- Nikhil Yadav
 * This program performs Kmeans clustering on a collection
 * from mongoDB
 */

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.BsonDocument;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

import org.apache.commons.math3.stat.descriptive.moment.GeometricMean;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class KMeans {

    int k;
    int no_of_dimensions;
    static String collection;
    static ArrayList<ArrayList<Double>> initcentroids;
    ArrayList<Double> max = new ArrayList<>();
    ArrayList<Double> min = new ArrayList<>();
    double sse=0.0;

    public KMeans(int k, MongoDatabase db, String collection) {
        this.k = k;
        this.collection = collection;

        FindIterable<Document> d = db.getCollection(collection).find().batchSize(1);
        for (Document docs : d) {
            this.no_of_dimensions = ((ArrayList<Double>) docs.get("point")).size();
            break;
        }

    }


    public double manhattan(Document a, Document b) {
        ArrayList<Double> doc1 = (ArrayList<Double>) a.get("point");
        ArrayList<Double> doc2 = (ArrayList<Double>) b.get("point");

        double distance = 0.0;
        for (int i = 0; i < doc1.size(); i++) {
            distance += Math.abs(doc1.get(i) - doc2.get(i));
        }
        return distance;
    }


    public double euclidean(ArrayList<Double> doc1, ArrayList<Double> b) {

        double distance = 0.0;
        for (int i = 0; i < doc1.size(); i++) {
            distance += Math.pow((doc1.get(i) - b.get(i)), 2);
        }
        return Math.sqrt(distance);
    }


    public boolean ShouldStop(ArrayList<ArrayList<Double>> old, ArrayList<ArrayList<Double>> centr, int iter, int maxIter) {
        if (iter > maxIter) return true;
        System.out.println("old is "+old);
        System.out.println("new is "+centr);
        System.out.println("epoch is "+iter);
        return old.containsAll(centr) && old.size() == centr.size();
    }



    public void Kmeans(MongoDatabase db, int epoch){
        ArrayList<ArrayList<Double>> centroids=initcentroids;

        ArrayList<ArrayList<Double>> oldCentroids=new ArrayList<>();
        int iterations=0;

        while(!ShouldStop(oldCentroids,centroids,iterations,epoch)){
            oldCentroids=(ArrayList<ArrayList<Double>>) centroids.clone();
            iterations+=1;

            getLabels(db,centroids);
            System.out.println("centroids before reshuffling "+centroids);
            centroids=getCentroids(db,centroids);
            System.out.println("centroids after reshuffling "+centroids);
        }
        System.out.println(centroids);
        for(int i=0;i<k;i++){
            Document movie =new Document();
            movie.append("point",centroids.get(i));
            movie.append("label",i);
            movie.append("isCentroid",true);
            if(i==0)movie.append("sse",sse);
            Bson filter2 = Filters.and(
                    Filters.eq("label", i),
                    Filters.eq("isCentroid", true)
            );
            package KMeans;
/**
 * @author- Nikhil Yadav
 * This program performs Kmeans clustering on a collection
 * from mongoDB
 */

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.BsonDocument;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

import org.apache.commons.math3.stat.descriptive.moment.GeometricMean;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class KMeans {

    int k;
    int no_of_dimensions;
    static String collection;
    static ArrayList<ArrayList<Double>> initcentroids;
    ArrayList<Double> max = new ArrayList<>();
    ArrayList<Double> min = new ArrayList<>();
    double sse=0.0;

    public KMeans(int k, MongoDatabase db, String collection) {
        this.k = k;
        this.collection = collection;

        FindIterable<Document> d = db.getCollection(collection).find().batchSize(1);
        for (Document docs : d) {
            this.no_of_dimensions = ((ArrayList<Double>) docs.get("point")).size();
            break;
        }

    }


    public double manhattan(Document a, Document b) {
        ArrayList<Double> doc1 = (ArrayList<Double>) a.get("point");
        ArrayList<Double> doc2 = (ArrayList<Double>) b.get("point");

        double distance = 0.0;
        for (int i = 0; i < doc1.size(); i++) {
            distance += Math.abs(doc1.get(i) - doc2.get(i));
        }
        return distance;
    }


    public double euclidean(ArrayList<Double> doc1, ArrayList<Double> b) {

        double distance = 0.0;
        for (int i = 0; i < doc1.size(); i++) {
            distance += Math.pow((doc1.get(i) - b.get(i)), 2);
        }
        return Math.sqrt(distance);
    }


    public boolean ShouldStop(ArrayList<ArrayList<Double>> old, ArrayList<ArrayList<Double>> centr, int iter, int maxIter) {
        if (iter > maxIter) return true;
        System.out.println("old is "+old);
        System.out.println("new is "+centr);
        System.out.println("epoch is "+iter);
        return old.containsAll(centr) && old.size() == centr.size();
    }



    public void Kmeans(MongoDatabase db, int epoch){
        ArrayList<ArrayList<Double>> centroids=initcentroids;

        ArrayList<ArrayList<Double>> oldCentroids=new ArrayList<>();
        int iterations=0;

        while(!ShouldStop(oldCentroids,centroids,iterations,epoch)){
            oldCentroids=(ArrayList<ArrayList<Double>>) centroids.clone();
            iterations+=1;

            getLabels(db,centroids);
            System.out.println("centroids before reshuffling "+centroids);
            centroids=getCentroids(db,centroids);
            System.out.println("centroids after reshuffling "+centroids);
        }
        System.out.println(centroids);
        for(int i=0;i<k;i++){
            Document movie =new Document();
            movie.append("point",centroids.get(i));
            movie.append("label",i);
            movie.append("isCentroid",true);
            if(i==0)movie.append("sse",sse);
            Bson filter2 = Filters.and(
                    Filters.eq("label", i),
                    Filters.eq("isCentroid", true)
            );
            if(db.getCollection(collection).countDocuments(filter2)==1) {
                db.getCollection(collection).updateOne(filter2,Updates.set("isCentroid",false));
            }
            db.getCollection(collection).insertOne(movie);
        }

    }


    public ArrayList<ArrayList<Double>> getRandomCentroids(MongoDatabase db) {

        FindIterable<Document> iter = db.getCollection(collection).find().batchSize(10000);
        MongoCursor<Document> cursor = iter.iterator();
        while (cursor.hasNext()) {
            Document o1 = cursor.next();
            ArrayList<Double> d1 = (ArrayList<Double>) o1.get("point");
            for (int i = 0; i < d1.size(); i++) {
                double d = d1.get(i);
                if (max.size() < no_of_dimensions && min.size() < no_of_dimensions) {
                    max.addAll(d1);
                    min.addAll(d1);
                    break;
                } else {
                    if (d > max.get(i)) max.set(i, d);
                    if (d < min.get(i)) min.set(i, d);
                }

            }
           // System.out.println(o1);

        }
       // System.out.println(max);
       // System.out.println(min);

        Random random = new Random();
        ArrayList<ArrayList<Double>> centroids = new ArrayList<>();
        for (int cl = 0; cl < this.k; cl++) {
            ArrayList<Double> centroid = new ArrayList<>();
            for (int i = 0; i < max.size(); i++) {
                double small = min.get(i);
                double large = max.get(i);
                centroid.add(i, small + ((large - small) * random.nextDouble()));
            }
            centroids.add(centroid);
        }
        System.out.println(centroids);
        initcentroids = centroids;
        return centroids;
    }

    public ArrayList<ArrayList<Double>> reinitializeCentroid(int index,ArrayList<ArrayList<Double>> centroids){
        Random random = new Random();
        ArrayList<Double> centroid=new ArrayList<>();
        for (int i = 0; i < max.size(); i++) {
            double small = min.get(i);
            double large = max.get(i);
            centroid.add(i, small + ((large - small) * random.nextDouble()));
        }
        centroids.set(index,centroid);
       // System.out.println("reinitialized at "+index+" to "+centroid);
        return centroids;
    }

    public void getLabels(MongoDatabase db, ArrayList<ArrayList<Double>> centroids) {
        double local_sse=0.0;
        FindIterable<Document> iter = db.getCollection(collection).find().batchSize(10000);
        MongoCursor<Document> cursor = iter.iterator();
        while (cursor.hasNext()) {
            int label = 0;
            double minValue = 0.0;
            boolean isCentroid=false;

            Document o1 = cursor.next();
            ArrayList<Double> d1 = (ArrayList<Double>) o1.get("point");

            if(centroids.contains(d1))isCentroid=true;

            for (int i = 0; i < centroids.size(); i++) {
                double temp = euclidean(d1, centroids.get(i));
                if (minValue == 0.0) minValue = temp;
                else if (minValue != 0.0 && minValue > temp) {
                    minValue = temp;
                    label = i;

                }
            }
            local_sse+=Math.pow(minValue,2);
            int id=o1.getInteger("_id");
            db.getCollection(collection).updateOne(eq("_id",id), Updates.set("label",label));
            db.getCollection(collection).updateOne(eq("_id",id), Updates.set("isCentroid",isCentroid));


        }
        sse=local_sse;
        System.out.println("SSE -"+sse);

    }



    public ArrayList<ArrayList<Double>> getCentroids(MongoDatabase db,ArrayList<ArrayList<Double>> centroids) {

        for (int i = 0; i < k; i++) {
            HashMap<Integer, ArrayList<Double>> map = new HashMap<>();
            ArrayList<Double> centroid = new ArrayList<>();
            FindIterable<Document> iter = db.getCollection(collection).find(eq("label", i)).batchSize(10000);

            MongoCursor<Document> cursor = iter.iterator();
            int counter = 0;
            while (cursor.hasNext()) {
                counter++;
                Document o1 = cursor.next();
                ArrayList<Double> d1 = (ArrayList<Double>) o1.get("point");
                for (int c = 0; c < d1.size(); c++) {
                    if (map.containsKey(c)) {
                        double d=d1.get(c);
                        ArrayList<Double> al=map.get(c);
                        al.add(d);
                        map.put(c, al);
                    } else {
                        ArrayList<Double> al=new ArrayList<>();
                        al.add(d1.get(c));
                        map.put(c,al );
                    }
                }

            }
            if(counter==0){
                System.out.println("index "+i+" before ="+centroids.get(i));
                centroids=reinitializeCentroid(i,centroids);
                System.out.println("index "+i+" after ="+centroids.get(i));
            }
            else {
                GeometricMean gm = new GeometricMean();
                for (int x = 0; x < no_of_dimensions; x++) {
                    ArrayList<Double> arr = map.get(x);
                    double[] gmean = new double[arr.size()];
                    for (int z = 0; z < arr.size(); z++) {
                        gmean[z] = arr.get(z);
                    }
                    centroid.add(x, gm.evaluate(gmean));
                }
                centroids.set(i,centroid);
            }
        }

        System.out.println(centroids);
        return centroids;
    }


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
        int K = Integer.parseInt(args[3]);
        int epochs = Integer.parseInt(args[4]);

        // https://www.w3resource.com/mongodb/mongodb-java-connection.php
        MongoClient mongoClient = getClient(mongoURL);
        MongoDatabase db = mongoClient.getDatabase(mongoDB);

        KMeans km = new KMeans(K, db, collections);
        km.getRandomCentroids(db);
        km.Kmeans(db,epochs);


    }
}
if(db.getCollection(collection).countDocuments(filter2)==1) {
                db.getCollection(collection).updateOne(filter2,Updates.set("isCentroid",false));
            }
            db.getCollection(collection).insertOne(movie);
        }

    }


    public ArrayList<ArrayList<Double>> getRandomCentroids(MongoDatabase db) {

        FindIterable<Document> iter = db.getCollection(collection).find().batchSize(10000);
        MongoCursor<Document> cursor = iter.iterator();
        while (cursor.hasNext()) {
            Document o1 = cursor.next();
            ArrayList<Double> d1 = (ArrayList<Double>) o1.get("point");
            for (int i = 0; i < d1.size(); i++) {
                double d = d1.get(i);
                if (max.size() < no_of_dimensions && min.size() < no_of_dimensions) {
                    max.addAll(d1);
                    min.addAll(d1);
                    break;
                } else {
                    if (d > max.get(i)) max.set(i, d);
                    if (d < min.get(i)) min.set(i, d);
                }

            }
           // System.out.println(o1);

        }
       // System.out.println(max);
       // System.out.println(min);

        Random random = new Random();
        ArrayList<ArrayList<Double>> centroids = new ArrayList<>();
        for (int cl = 0; cl < this.k; cl++) {
            ArrayList<Double> centroid = new ArrayList<>();
            for (int i = 0; i < max.size(); i++) {
                double small = min.get(i);
                double large = max.get(i);
                centroid.add(i, small + ((large - small) * random.nextDouble()));
            }
            centroids.add(centroid);
        }
        System.out.println(centroids);
        initcentroids = centroids;
        return centroids;
    }

    public ArrayList<ArrayList<Double>> reinitializeCentroid(int index,ArrayList<ArrayList<Double>> centroids){
        Random random = new Random();
        ArrayList<Double> centroid=new ArrayList<>();
        for (int i = 0; i < max.size(); i++) {
            double small = min.get(i);
            double large = max.get(i);
            centroid.add(i, small + ((large - small) * random.nextDouble()));
        }
        centroids.set(index,centroid);
       // System.out.println("reinitialized at "+index+" to "+centroid);
        return centroids;
    }

    public void getLabels(MongoDatabase db, ArrayList<ArrayList<Double>> centroids) {
        double local_sse=0.0;
        FindIterable<Document> iter = db.getCollection(collection).find().batchSize(10000);
        MongoCursor<Document> cursor = iter.iterator();
        while (cursor.hasNext()) {
            int label = 0;
            double minValue = 0.0;
            boolean isCentroid=false;

            Document o1 = cursor.next();
            ArrayList<Double> d1 = (ArrayList<Double>) o1.get("point");

            if(centroids.contains(d1))isCentroid=true;

            for (int i = 0; i < centroids.size(); i++) {
                double temp = euclidean(d1, centroids.get(i));
                if (minValue == 0.0) minValue = temp;
                else if (minValue != 0.0 && minValue > temp) {
                    minValue = temp;
                    label = i;

                }
            }
            local_sse+=Math.pow(minValue,2);
            int id=o1.getInteger("_id");
            db.getCollection(collection).updateOne(eq("_id",id), Updates.set("label",label));
            db.getCollection(collection).updateOne(eq("_id",id), Updates.set("isCentroid",isCentroid));


        }
        sse=local_sse;
        System.out.println("SSE -"+sse);

    }



    public ArrayList<ArrayList<Double>> getCentroids(MongoDatabase db,ArrayList<ArrayList<Double>> centroids) {

        for (int i = 0; i < k; i++) {
            HashMap<Integer, ArrayList<Double>> map = new HashMap<>();
            ArrayList<Double> centroid = new ArrayList<>();
            FindIterable<Document> iter = db.getCollection(collection).find(eq("label", i)).batchSize(10000);

            MongoCursor<Document> cursor = iter.iterator();
            int counter = 0;
            while (cursor.hasNext()) {
                counter++;
                Document o1 = cursor.next();
                ArrayList<Double> d1 = (ArrayList<Double>) o1.get("point");
                for (int c = 0; c < d1.size(); c++) {
                    if (map.containsKey(c)) {
                        double d=d1.get(c);
                        ArrayList<Double> al=map.get(c);
                        al.add(d);
                        map.put(c, al);
                    } else {
                        ArrayList<Double> al=new ArrayList<>();
                        al.add(d1.get(c));
                        map.put(c,al );
                    }
                }

            }
            if(counter==0){
                System.out.println("index "+i+" before ="+centroids.get(i));
                centroids=reinitializeCentroid(i,centroids);
                System.out.println("index "+i+" after ="+centroids.get(i));
            }
            else {
                GeometricMean gm = new GeometricMean();
                for (int x = 0; x < no_of_dimensions; x++) {
                    ArrayList<Double> arr = map.get(x);
                    double[] gmean = new double[arr.size()];
                    for (int z = 0; z < arr.size(); z++) {
                        gmean[z] = arr.get(z);
                    }
                    centroid.add(x, gm.evaluate(gmean));
                }
                centroids.set(i,centroid);
            }
        }

        System.out.println(centroids);
        return centroids;
    }


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
        int K = Integer.parseInt(args[3]);
        int epochs = Integer.parseInt(args[4]);

        // https://www.w3resource.com/mongodb/mongodb-java-connection.php
        MongoClient mongoClient = getClient(mongoURL);
        MongoDatabase db = mongoClient.getDatabase(mongoDB);

        KMeans km = new KMeans(K, db, collections);
        km.getRandomCentroids(db);
        km.Kmeans(db,epochs);


    }
}
