package FillFromWikidata;
/**
 * This program updates the sources by taking new info from
 * wikipedia json file. It performs data cleaning of wikipedia.json
 * and integrates it with mongo imdb collection
 * @author -Nikhil Yadav
 */

//imports necessary mongo libraries and JSON Parser
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;

public class FillFromWikidata {
	
    // map and sets created for the purpose of avoiding duplicates and mapping data while cleaning
    static Connection conn;
    static HashSet<Integer> set = new HashSet<>();
    static HashMap<Integer, Integer> map = new HashMap<>();
    static HashMap<Integer, String> currencyMap = new HashMap<>();
    static HashSet<Integer> comedyset = new HashSet<>();
    static HashSet<Integer> actiondramaset = new HashSet<>();
    static HashMap<Integer, String> colorMap = new HashMap<>();
    static HashMap<Integer,Integer> comcopy = new HashMap<>();
    static HashMap<Integer,Integer> actcopy = new HashMap<>();
    static HashMap<Integer,Integer> youngcopy = new HashMap<>();

   	/**
   	* Method to find deathcause of actor if dead
   	**/
    public void deathcause(String folder, MongoDatabase db) throws SQLException, IOException, ParseException {
        @SuppressWarnings("unchecked")
        ResultSet rs;
        Statement st = conn.createStatement();
        rs = st.executeQuery("SELECT id FROM YoungActor");
        ArrayList<Integer> duplicates = new ArrayList<>();
        while (rs.next()) {
            int id = rs.getInt("id");
            set.add(id);
        }
        rs.close();
        st.close();

        Scanner sc = new Scanner(new FileReader(folder));
        int count = 0;
        int caucount = 0;
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            JSONObject obj = null;
            JSONObject jo = obj;
            try {
                obj = (JSONObject) new JSONParser().parse(line);
                jo = obj;
            } catch (Exception e) {
            }

            String actor = null;
            String id = null;
            String cause_id = null;
            String causeLabel = null;
            String currency = null;
            String symbol = null;
            String color = null;
            String colorDesc = null;
            int colorid = 0;
            int temp = 0;

            try {
                color = (String) jo.get("color");
                colorDesc = (String) jo.get("colorDescription");
                if (!(color == null) && !(colorDesc == null)) {
                        color = color.replaceAll("[^0-9]", "");
                        colorid = Integer.parseInt(color);
                        if(colorMap.containsKey(colorid)&&colorMap.get(colorid).startsWith("visual")){

                        }
                        else {
                            colorMap.put(colorid, colorDesc);
                        }


                }
            } catch (Exception e) {
            }


            try {
                currency = (String) jo.get("currency");
                symbol = (String) jo.get("symbol");
                if (!(currency == null)) {
                    currency = currency.replaceAll("[^0-9]", "");
                    int curr = Integer.parseInt(currency);
                    if (currencyMap.containsKey(curr)&& !symbol.equals(currencyMap.get(curr))) {
                        duplicates.add(curr);
                    }
                    currencyMap.put(curr, symbol);

                }
            } catch (NullPointerException e) {
            }

            int tempact = 0;
            int cause = 0;
            try {
                actor = (String) jo.get("actor");
            } catch (NullPointerException e) {
            }
            if (!(actor == null)) {
                actor = actor.replaceAll("[^0-9]", "");
                tempact = Integer.parseInt(actor);
            }

            try {
                id = (String) jo.get("id");
            } catch (NullPointerException e) {
            }

            if (!(id == null) && !(actor == null)) {
                String numberOnly = id.replaceAll("[^0-9]", "");
                temp = Integer.parseInt(numberOnly);

                if (set.contains(temp)) {
                    //  System.out.println("found id " + temp);
                    map.put(Integer.parseInt(actor), temp);
                    count++;
                    Document p = new Document();
                    p.append("sql_id", temp);
                    p.append("_id", tempact);
                    db.getCollection("YoungActor").insertOne(p);
                }
            }
            try {
                cause_id = (String) jo.get("cause");
            } catch (NullPointerException e) {
            }
            if (!(cause_id == null)) {
                cause_id = cause_id.replaceAll("[^0-9]", "");
                cause = Integer.parseInt(cause_id);
                if (map.containsKey(tempact)) caucount++;
                db.getCollection("YoungActor").updateOne(new BasicDBObject("_id", tempact),
                        new BasicDBObject("$set", new BasicDBObject("cause_id", cause)));
                FindIterable<Document> val = db.getCollection("YoungActor").find(eq("_id", tempact)).projection(include("sql_id"));
                for (Document doc : val) {
                    //access documents e.g. doc.get()
                    int v = doc.getInteger("sql_id");
                    youngcopy.put(v,cause);
                }
            }
            try {
                causeLabel = (String) jo.get("causeLabel");
            } catch (NullPointerException e) {
            }
            if (!(causeLabel == null) && !(cause_id == null)) {
                db.getCollection("YoungActor").updateMany(eq("cause_id", cause), Updates.set("causeLabel",
                        causeLabel));

            }

        }
        sc.close();
        System.out.println(caucount);
        System.out.println(map.size());
        for (int i : duplicates) {
            currencyMap.remove(i);
        }

        System.out.println("currency map");
        for (int i : currencyMap.keySet()) {
            System.out.println(i + "-" + currencyMap.get(i));
        }

        System.out.println("\n ColorMap");
        for (int i : colorMap.keySet()) {
            System.out.println(i + "-" + colorMap.get(i));
        }
        set = null;
        duplicates = null;
        System.out.println(youngcopy.size());

    }

	/**
	* Method to fill box office collection of movie and color of movie of particular genres
	**/
    public void MoviesBoxOfficeAndColor(String folder, MongoDatabase db) throws SQLException, FileNotFoundException {
        ResultSet rs;
        ResultSet rs1;
        Statement st = conn.createStatement();

        rs = st.executeQuery("SELECT id FROM ComedyMovie");

        while (rs.next()) {
            int id = rs.getInt("id");
            comedyset.add(id);

        }
        rs.close();

        rs1 = st.executeQuery("SELECT id FROM ActionDramaMovie");
        while (rs1.next()) {
            int id = rs1.getInt("id");
            actiondramaset.add(id);
        }
        rs1.close();
        System.out.println(comedyset.size());
        System.out.println(actiondramaset.size());
        st.close();

        Scanner sc = new Scanner(new FileReader(folder));

        while (sc.hasNextLine()) {

            // System.out.println(mcheck);
            String line = sc.nextLine();
            //System.out.println(line);
            JSONObject obj = null;
            JSONObject jo = obj;
            try {
                obj = (JSONObject) new JSONParser().parse(line);
                jo = obj;
            } catch (Exception e) {
            }
            String movie = null;
            String id = null;
            String boxOffice = null;
            String boxUnit = null;
            String color = null;
            int movint = 0;
            int idint = 0;
            int colorid = 0;
            int boxu = 0;
            int boxunitid = 0;
            try {
                movie = (String) jo.get("movie");
                if (!(movie == null)) {
                    movie = movie.replaceAll("[^0-9]", "");
                    movint = Integer.parseInt(movie);
                }
            } catch (Exception e) {
            }

            try {
                id = (String) jo.get("id");
                if (!(id == null) && id.startsWith("h")) {
                    String test[] = id.split("/");
                    try {
                        idint = Integer.parseInt(test[4].substring(2));
                    } catch (Exception e) {
                        String temp[] = test[4].substring(2).split("[?]");
                        System.out.println(temp[0]);
                        idint = Integer.parseInt(temp[0]);
                    }
                } else if (!(id == null)) {
                    id = id.replaceAll("[^0-9]", "");
                    idint = Integer.parseInt(id);
                }

            } catch (Exception e) {
            }
            try {
                if (!(movie == null) && !(id == null)) {
                    if (comedyset.contains(idint)) {
                        Document p = new Document();
                        p.append("sql_id", idint);
                        p.append("_id", movint);
                        db.getCollection("ComedyMovie").insertOne(p);
                        //Qmoviecomset.add(movint);
                        //comedyset.remove(idint);
                    }
                    if (actiondramaset.contains(idint)) {
                        Document p = new Document();
                        p.append("sql_id", idint);
                        p.append("_id", movint);
                        db.getCollection("ActionDramaMovie").insertOne(p);
                        if(idint==245990||idint==246809){
                            System.out.println(movint);
                        }
                        //  Qmovieset.add(movint);
                        //actiondramaset.remove(idint);
                    }
                }
            } catch (Exception e) {
            }

            try {
                color = (String) jo.get("color");
                color = color.replaceAll("[^0-9]", "");
                colorid = Integer.parseInt(color);
            } catch (Exception e) {
            }

            if (!(movie == null) && !(color == null)) {

                if (db.getCollection("ActionDramaMovie").countDocuments(eq("_id", movint)) == 1) {
                    FindIterable<Document> val = db.getCollection("ActionDramaMovie").find(eq("_id", movint)).projection(include("sql_id"));
                    for (Document doc : val) {
                        int v = doc.getInteger("sql_id");
                        actcopy.put(v,movint);
                    }
                    db.getCollection("ActionDramaMovie").updateOne(eq("_id", movint),
                            Updates.set("colorDesc", colorMap.get(colorid)));
                    //actiondramacolor++;
                }
            }


            try {
                boxOffice = (String) jo.get("boxoffice");
                boxUnit = (String) jo.get("boxofficeUnit");
            } catch (Exception e) {
            }

            if (!(boxOffice == null)) {
                boxunitid = Integer.parseInt(boxUnit.replaceAll("[^0-9]", ""));
                if (currencyMap.containsKey(boxunitid)) {
                    String value = currencyMap.get(boxunitid) + boxOffice;
                    if (db.getCollection("ComedyMovie").countDocuments(eq("_id", movint)) == 1) {
                        FindIterable<Document> val = db.getCollection("ComedyMovie").find(eq("_id", movint)).projection(include("sql_id"));
                        for (Document doc : val) {
                            int v = doc.getInteger("sql_id");
                            comcopy.put(v,movint);
                        }
                        db.getCollection("ComedyMovie").updateOne(eq("_id", movint),
                                Updates.set("boxoffice", value));
                    }

                }
            }

        }
        sc.close();
        System.out.println(actcopy.size());
        System.out.println(comcopy.size());
        comedyset = null;
        actiondramaset = null;
        currencyMap = null;
        colorMap = null;
    }

    /**
    * Updating the sql tables of movies of particular genres based on new cleaned info 
    **/
    public void AddtoSQL(MongoDatabase db) throws SQLException {     
        PreparedStatement st = conn.prepareStatement("UPDATE ComedyMovie SET boxoffice = ? WHERE id= ?");
        int counter=0;
        for (int i : comcopy.keySet()) {
            FindIterable<Document> val = db.getCollection("ComedyMovie").find(eq("sql_id", i));
            String box = null;
            for (Document doc : val) {
                box = doc.getString("boxoffice");
                st.setString(1, box);
                st.setInt(2, i);
                st.executeUpdate();
                counter++;
            }
        }

        System.out.println(counter);
        counter=0;
        st = conn.prepareStatement("UPDATE ActionDramaMovie SET color = ? WHERE id= ?");
        for(int i:actcopy.keySet()){
            counter++;
            FindIterable<Document> val = db.getCollection("ActionDramaMovie").find(eq("_id", actcopy.get(i)));
            String color = null;
            for (Document doc : val) {
                color = doc.getString("colorDesc");
                st.setString(1, color);
                st.setInt(2, i);
                st.executeUpdate();
            }
        }

        System.out.println(counter);
        counter=0;

        st = conn.prepareStatement("UPDATE YoungActor SET deathcause = ? WHERE id= ?");
        for(int i:youngcopy.keySet()){
            counter++;
            FindIterable<Document> val = db.getCollection("YoungActor").find(eq("sql_id", i));
            String dc = null;
            for (Document doc : val) {
                dc = doc.getString("causeLabel");
                st.setString(1, dc);
                st.setInt(2, i);
                st.executeUpdate();
            }
        }
        st.executeBatch();
        System.out.println(counter);



    }


    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }


    public static void main(String[] args) throws SQLException, IOException, ParseException {
        String folder = args[0];
        String dbURL = args[1];
        String user = args[2];
        String pass = args[3];
        String mongoURL = args[4];
        String mongoDB = args[5];

        conn = DriverManager.getConnection(dbURL, user, pass);

        MongoClient mongoClient = getClient(mongoURL);
        MongoDatabase db = mongoClient.getDatabase(mongoDB);
        // create new collections 
        db.createCollection("YoungActor");
        db.createCollection("ComedyMovie");
        db.createCollection("ActionDramaMovie");
        FillFromWikidata ffw = new FillFromWikidata();

	// filling values in new collections
        ffw.deathcause(folder, db);
        ffw.MoviesBoxOfficeAndColor(folder, db);
        ffw.AddtoSQL(db);
        db.drop();
    }
}
