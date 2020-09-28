package ComputeMutualInformation;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;

public class ComputeMutualInformation {
    int Ui;
    int Vk;
    String collection;
    int total;
    double hu=0.0;
    double hv=0.0;
    double mi=0.0;


    public ComputeMutualInformation(MongoDatabase db,String collection){
        MongoCursor<Integer> al =db.getCollection(collection).distinct("label",Integer.class).iterator();
        int count=0;
        while(al.hasNext()){
            count++;
            al.next();
        }
        this.Ui=count;
        this.collection=collection;
        System.out.println("no of clusters in U "+Ui);


         al =db.getCollection(collection).distinct("expected",Integer.class).iterator();
        count=0;
        while(al.hasNext()){
            count++;
            al.next();
        }
        this.Vk=count;
        this.collection=collection;
        System.out.println("no of clusters in V "+Vk);

        Bson filter = Filters.and(
                Filters.eq("isCentroid", false)
        );
        this.total=(int)db.getCollection(collection).countDocuments(filter);
    }



    public void HuHv(MongoDatabase db,String collection){

        for(int i=0;i<Ui;i++) {
            Bson filter = Filters.and(
                    Filters.eq("label", i),
                    Filters.eq("isCentroid", false)
            );
            double no_docs = (double) db.getCollection(collection).countDocuments(filter);

            hu+=(no_docs/total)*Math.log(no_docs/total);

            //umap.put(i, no_docs);
        }
        hu=-1.0*hu;
        System.out.println(hu);
        for(int i=0;i<Vk;i++){
            Bson filter2 = Filters.and(
                    Filters.eq("expected", i),
                    Filters.eq("isCentroid", false)
            );
            double no_docs=(double)db.getCollection(collection).countDocuments(filter2);
            hv+=(no_docs/total)*Math.log(no_docs/total);
            //vmap.put(i,no_docs);

        }
        hv=-1.0*hv;
        System.out.println(hv);

    }

    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }

    public int intersection(MongoDatabase db,MongoCursor<Document> cursor1, int cluster_no, String label){
        int count=0;
        while(cursor1.hasNext()){
            Document o1=cursor1.next();
            int id1=o1.getInteger("_id");

            Bson filter = Filters.and(
                    Filters.eq(label, cluster_no),
                    Filters.eq("isCentroid", false),
                    Filters.eq("_id",id1)
            );
            count+=db.getCollection(collection).countDocuments(filter);

        }
        return count;
    }

    public void MICalc(MongoDatabase db){
        for(int i=0;i<Ui;i++){
            Bson filter = Filters.and(
                    Filters.eq("label", i),
                    Filters.eq("isCentroid", false)
            );
            double no_docs = (double) db.getCollection(collection).countDocuments(filter);

            for(int j=0;j<Vk;j++){
                Bson filter2 = Filters.and(
                        Filters.eq("expected", j),
                        Filters.eq("isCentroid", false)
                );
                double no_docs2 = (double) db.getCollection(collection).countDocuments(filter2);
                if(no_docs>no_docs2){
                    FindIterable<Document> iter=db.getCollection(collection).find(filter2).batchSize(10000);
                    MongoCursor<Document> cursor=iter.iterator();
                    int common=intersection(db,cursor,i,"label");
                    if(common!=0){
                        double ratio=(double)common/total;
                        double pu=no_docs/total;
                        double pv=no_docs2/total;
                        mi+=(ratio)*Math.log(ratio/(pu*pv));
                    }
                    System.out.println("common between "+i+":"+j+" is "+common);
                }
                else{
                    FindIterable<Document> iter=db.getCollection(collection).find(filter).batchSize(10000);
                    MongoCursor<Document> cursor=iter.iterator();
                    int common=intersection(db,cursor,j,"expected");
                    if(common!=0){
                        double ratio=(double)common/total;
                        double pu=no_docs/total;
                        double pv=no_docs2/total;
                        mi+=(ratio)*Math.log(ratio/(pu*pv));
                    }
                    System.out.println("common between "+i+":"+j+" is "+common);
                }
            }

        }
        System.out.println(mi);
    }

    public void WriteToFile(String file) throws FileNotFoundException {
        PrintWriter pw=new PrintWriter(file);
        pw.write(hu+"");
        pw.write("\n");
        pw.write(hv+"");
        pw.write("\n");
        pw.write(mi+"");
        pw.flush();
        pw.close();
    }

    public static void main(String[] args) throws FileNotFoundException {
        String mongoURL = args[0];
        String mongoDB = args[1];
        String collections = args[2];
        String output=args[3];

        // https://www.w3resource.com/mongodb/mongodb-java-connection.php
        MongoClient mongoClient = getClient(mongoURL);
        MongoDatabase db = mongoClient.getDatabase(mongoDB);

        ComputeMutualInformation mi=new ComputeMutualInformation(db,collections);
        mi.HuHv(db,collections);
        mi.MICalc(db);
        mi.WriteToFile(output);
    }
}
