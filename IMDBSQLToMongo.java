
package IMDBSQLToMongo;
/**
 * This program migrates sql imdb_ibd data to mongodb
 * @author - Nikhil yadav
 */

// importing necessary mongodB libraries
import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import com.mongodb.client.model.Updates;
import static com.mongodb.client.model.Filters.eq;


public class IMDBSQLToMongo {
    static Connection conn = null;

    /**
     *Fills the person collection from person table
     * @param db
     * 
     */
    public void personFill(MongoDatabase db) throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(10000);
        rs = stmt.executeQuery("SELECT * FROM person ");

        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int birthYear = rs.getInt("birthYear");
            int deathYear = rs.getInt("deathYear");


            Document movie = new Document();
            movie.append("_id", id);
            movie.append("name", name);
            if (birthYear != 0) movie.append("birthYear", birthYear);
            if (deathYear != 0) movie.append("deathYear", deathYear);

            //https://stackoverflow.com/questions/15371839/how-to-add-an-array-to-a-mongodb-document-using-java
            List<String> writer = new ArrayList<>();
            List<String> producer = new ArrayList<>();
            List<String> actor = new ArrayList<>();
            List<String> director = new ArrayList<>();
            List<String> composer = new ArrayList<>();
            List<String> editor = new ArrayList<>();

            movie.append("actor", actor);
            movie.append("composer", composer);
            movie.append("director", director);
            movie.append("editor", editor);
            movie.append("producer", producer);
            movie.append("writer", writer);

            db.getCollection("People").insertOne(movie);

            if (id % 10000 == 0) System.out.println(id);
        }
        System.out.println("\n");
        System.out.println("Person inserted into check People");
        stmt.close();
        rs.close();


    }


    /**
     * Method to fill the movies collection from mongodb after extracting
     * sql data from movie table
     *
     * @param db
     * @throws SQLException
     */
    public void MovieFill(MongoDatabase db) throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(10000);
        rs = stmt.executeQuery("SELECT * FROM movie ");
        //ResultSetMetaData rsmd = rs.getMetaData();
        //int columnsNumber = rsmd.getColumnCount();
        while (rs.next()) {
            int id = rs.getInt("id");
            String title = rs.getString("title");
            int releaseYear = rs.getInt("releaseYear");
            int runtime = rs.getInt("runtime");
            float rating = rs.getFloat("rating");
            int numberOfVotes = rs.getInt("numberOfVotes");
            Document movie = new Document();
            movie.append("_id", id);
            movie.append("title", title);
            if (releaseYear != 0) movie.append("releaseYear", releaseYear);
            if (runtime != 0) movie.append("runtime", runtime);
            if (rating != 0) movie.append("rating", rating);
            if (numberOfVotes != 0) movie.append("numberOfVotes", numberOfVotes);

            List<String> genre = new ArrayList<>();
            movie.append("genres", genre);
            db.getCollection("Movies").insertOne(movie);

            if (id % 10000 == 0) System.out.println("movie id= " + id);
        }
        System.out.println("all movies inserted into check movies");
        stmt.close();
        rs.close();


    }
    
    /**
     * Method to fill the producer list  in person from mongodb after extracting
     * sql data from producedby table
     *
     * @param db
     * @throws SQLException
     */
    public void producerFill(MongoDatabase db) throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(10000);
        rs = stmt.executeQuery("SELECT * FROM producedby ");

        int count = 0;
        while (rs.next()) {
            int pid = rs.getInt("personId");
            int mid = rs.getInt("movieId");

            //https://stackoverflow.com/questions/15436542/mongodb-java-push-into-array
            db.getCollection("People").updateOne(eq("_id", pid), Updates.addToSet("producer", mid));
            if (count++ % 10000 == 0) System.out.println("producer= " + count);
        }
        stmt.close();
        rs.close();
    }

     /**
     * Method to fill the editor list  in person from mongodb after extracting
     * sql data from editedby table
     *
     * @param db
     * @throws SQLException
     */
    public void editorFill(MongoDatabase db) throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(10000);
        rs = stmt.executeQuery("SELECT * FROM editedby ");

        int count = 0;
        while (rs.next()) {
            int pid = rs.getInt("personId");
            int mid = rs.getInt("movieId");

            db.getCollection("People").updateOne(eq("_id", pid), Updates.addToSet("editor", mid));
            if (count++ % 10000 == 0) System.out.println("Editor = " + count);
        }
        stmt.close();
        rs.close();

    }
	
    /**
     * Method to fill the actor list  in person from mongodb after extracting
     * sql data from actedin table
     *
     * @param db
     * @throws SQLException
     */
    public void actorFill(MongoDatabase db) throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(10000);
        rs = stmt.executeQuery("SELECT * FROM actedin ");

        int count = 0;
        while (rs.next()) {
            int pid = rs.getInt("personId");
            int mid = rs.getInt("movieId");
            db.getCollection("People").updateOne(eq("_id", pid), Updates.addToSet("actor", mid));
            if (count++ % 10000 == 0) System.out.println("actor=" + count);
        }
        stmt.close();
        rs.close();

    }


   /**
     * Method to fill the director list  in person from mongodb after extracting
     * sql data from directedby table
     *
     * @param db
     * @throws SQLException
     */
    public void directorFill(MongoDatabase db) throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(10000);
        rs = stmt.executeQuery("SELECT * FROM directedby ");
        //ResultSetMetaData rsmd = rs.getMetaData();
        //int columnsNumber = rsmd.getColumnCount();
        int count = 0;
        while (rs.next()) {
            int pid = rs.getInt("personId");
            int mid = rs.getInt("movieId");
            db.getCollection("People").updateOne(eq("_id", pid), Updates.addToSet("director", mid));
            if (count++ % 10000 == 0) System.out.println("director=" + count);
        }
        stmt.close();
        rs.close();
    }

    /**
     * Method to fill the writer list  in person from mongodb after extracting
     * sql data from writtenby table
     *
     * @param db
     * @throws SQLException
     */
    public void writerFill(MongoDatabase db) throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(10000);
        rs = stmt.executeQuery("SELECT * FROM writtenby ");

        int count = 0;
        while (rs.next()) {
            int pid = rs.getInt("personId");
            int mid = rs.getInt("movieId");

            db.getCollection("People").updateOne(eq("_id", pid), Updates.addToSet("writer", mid));
            if (count++ % 10000 == 0) System.out.println("writer=" + count);
        }
        stmt.close();
        rs.close();

    }


    /**
     * Method to fill the composer list  in person from mongodb after extracting
     * sql data from composedby table
     *
     * @param db
     * @throws SQLException
     */
    public void composerFill(MongoDatabase db) throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(10000);
        rs = stmt.executeQuery("SELECT * FROM composedby ");
        int count = 0;
        while (rs.next()) {
            int pid = rs.getInt("personId");
            int mid = rs.getInt("movieId");

            db.getCollection("People").updateOne(eq("_id", pid), Updates.addToSet("composer", mid));
            if (count++ % 10000 == 0) System.out.println("composer =" + count);
        }
        stmt.close();
        rs.close();

    }

    /**
     * Method to fill the genre list  in movies from mongodb after extracting
     * sql data from hasGenre and genre table after join 
     *
     * @param db
     * @throws SQLException
     */
    public void HasGenreFill(MongoDatabase db) throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(10000);
        rs = stmt.executeQuery("select hasgenre.movieId,genre.name from hasgenre join genre on hasgenre.genreId=genre.id ");
        int count = 0;
        while (rs.next()) {
            int mid = rs.getInt("movieId");
            String name = rs.getString("name");
            db.getCollection("Movies").updateOne(eq("_id", mid), Updates.addToSet("genres", name));
            if (count++ % 1000 == 0) System.out.println("genre = " + count);

        }
        stmt.close();
        rs.close();

    }

    //https://mycourses.rit.edu/d2l/le/content/817251/viewContent/6254218/View
    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }

    // https://coderwall.com/p/609ppa/printing-the-result-of-resultset
    public static void main(String[] args) throws SQLException {
        String dbURL = args[0];
        String user = args[1];
        String pass = args[2];
        String mongoURL = args[3];
        String mongoDB = args[4];

        conn = DriverManager.getConnection(dbURL, user, pass);


        // https://www.w3resource.com/mongodb/mongodb-java-connection.php
        MongoClient mongoClient = getClient(mongoURL);
        MongoDatabase db = mongoClient.getDatabase(mongoDB);
        db.createCollection("Movies");
        db.createCollection("People");

        IMDBSQLToMongo mongo = new IMDBSQLToMongo();
        // calling all the functions to fill the collections from sql table
        mongo.MovieFill(db);
        mongo.personFill(db);
        mongo.producerFill(db);
        mongo.actorFill(db);
        mongo.composerFill(db);
        mongo.writerFill(db);
        mongo.editorFill(db);
        mongo.directorFill(db);
        mongo.HasGenreFill(db);
        conn.close();
    }
}






