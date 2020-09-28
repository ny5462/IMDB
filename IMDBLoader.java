
/**
 * This program conencts to mysql via jdbc and loads IMDB dataset on it,
 * and also creates a given schema that maps proffessions of certain
 * existing individuals working in movies.
 *
 * @ Author- Nikhil Yadav
 */


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 *  class that loads imdb dataset and creates schema
 */
class IMDBLoader {

    static String common = "";

    static int gcount = 1;
    HashMap<String, Integer> map = new HashMap<>();
    static String databaseURL = "";
    static Connection conn = null;
    static String user = "";
    static String pass = "";

    /**
     * creates tables in dataset
     */
    public void createTables() {

        PreparedStatement st = null;
        try {

            st = conn.prepareStatement("CREATE TABLE Person(id INT NOT NULL, name VARCHAR(200),birthYear INT," +
                    "deathYear INT, PRIMARY KEY(id))");
            st.executeUpdate();
            st = conn.prepareStatement("CREATE TABLE Movie(id INT NOT NULL,title VARCHAR(500),releaseYear INT, " +
                    "runtime INT,rating FLOAT, numberOfVotes INT, PRIMARY KEY(id))");
            st.executeUpdate();
            st = conn.prepareStatement("CREATE TABLE Genre(id  INT NOT NULL, name VARCHAR(200),PRIMARY KEY(id))");
            st.executeUpdate();
            st = conn.prepareStatement("CREATE TABLE ActedIn(personId  INT , movieId INT , PRIMARY KEY(personId,movieId)" +
                    ", FOREIGN KEY(personId) REFERENCES Person(id), FOREIGN KEY(movieId) REFERENCES Movie(id))");
            st.executeUpdate();
            st = conn.prepareStatement("CREATE TABLE ComposedBy(personId  INT , movieId INT , PRIMARY KEY(personId,movieId)," +
                    "FOREIGN KEY(personId) REFERENCES Person(id), FOREIGN KEY(movieId) REFERENCES Movie(id))");
            st.executeUpdate();
            st = conn.prepareStatement("CREATE TABLE DirectedBy(personId  INT , movieId INT, PRIMARY KEY(personId,movieId)," +
                    "FOREIGN KEY(personId) REFERENCES Person(id), FOREIGN KEY(movieId) REFERENCES Movie(id))");
            st.executeUpdate();
            st = conn.prepareStatement("CREATE TABLE ProducedBy(personId  INT , movieId INT ,PRIMARY KEY(personId,movieId)," +
                    "FOREIGN KEY(personId) REFERENCES Person(id), FOREIGN KEY(movieId) REFERENCES Movie(id))");
            st.executeUpdate();
            st = conn.prepareStatement("CREATE TABLE EditedBy(personId  INT , movieId INT,PRIMARY KEY(personId,movieId)," +
                    "FOREIGN KEY(personId) REFERENCES Person(id), FOREIGN KEY(movieId) REFERENCES Movie(id))");
            st.executeUpdate();
            st = conn.prepareStatement("CREATE TABLE WrittenBy(personId  INT , movieId INT ,PRIMARY KEY(personId,movieId)," +
                    "FOREIGN KEY(personId) REFERENCES Person(id), FOREIGN KEY(movieId) REFERENCES Movie(id))");
            st.executeUpdate();
            st = conn.prepareStatement("CREATE TABLE HasGenre(genreId  INT , movieId INT ,FOREIGN KEY(genreId) " +
                    "REFERENCES Genre(id), FOREIGN KEY(movieId) REFERENCES Movie(id))");
            st.executeUpdate();
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("tables generated");
    }

    /**
     * loads person in dataset
     * @throws IOException
     */
    public void person() throws IOException {


        InputStream gzinput = new GZIPInputStream(new FileInputStream(common + "name.basics.tsv.gz"));
        //file 1
        try {

            conn.setAutoCommit(false);
            Scanner sc = new Scanner(gzinput, "UTF-8");
            sc.nextLine();
            PreparedStatement st = conn.prepareStatement("INSERT INTO Person(id,name,birthYear,deathYear)" +
                    "VALUES(?,?,?,?)");
            int batch = 1;
            while (sc.hasNextLine()) {
                int count = 0;
                while (count != 10000 && sc.hasNextLine()) {
                    String temp = sc.nextLine();
                    String[] arr = temp.split("\t");

                    try {
                        int id = Integer.parseInt(arr[0].substring(2));
                        String name = arr[1];
                        int birth;
                        int death;
                        try {
                            birth = Integer.parseInt(arr[2]);
                            st.setInt(3, birth);
                        } catch (Exception e) {
                            st.setNull(3, Types.NULL);
                        }
                        try {
                            death = Integer.parseInt(arr[3]);
                            st.setInt(4, death);

                        } catch (Exception e) {
                            st.setNull(4, Types.NULL);
                        }

                        st.setInt(1, id);
                        st.setString(2, name);


                        st.addBatch();
                    } catch (NumberFormatException e) {
                        continue;

                    }
                    count++;
                }

                System.out.println("batch no:" + batch + " total = " + batch * 10000);
                st.executeBatch();
                batch++;
                conn.commit();
                // if(batch==11)break;
            }
            st.close();
            sc.close();
            if (conn != null) {
                System.out.println("Connected to the database");
            }
        } catch (SQLException ex) {
            System.out.println("An error occurred. Maybe user/password is invalid");
            ex.printStackTrace();
        }
    }

    /**
     * adds ratings and numberofvotes to movies
     * @throws IOException
     */
    public void updateMovies() throws IOException {

        InputStream gzinput = new GZIPInputStream(new FileInputStream(common + "title.ratings.tsv.gz"));
        //file 1
        try {
            conn.setAutoCommit(false);
            Scanner sc = new Scanner(gzinput, "UTF-8");
            sc.nextLine();
            PreparedStatement st = conn.prepareStatement("UPDATE Movie SET rating = ? WHERE id= ?");
            PreparedStatement st1 = conn.prepareStatement("UPDATE Movie SET numberOfVotes = ? WHERE id=?");
            int batch = 1;
            int id;
            while (sc.hasNextLine()) {
                int count = 0;
                while (count != 10000 && sc.hasNextLine()) {
                    String temp = sc.nextLine();
                    String[] arr = temp.split("\t");

                    try {
                        id = Integer.parseInt(arr[0].substring(2));
                        st.setFloat(1, Float.parseFloat(arr[1]));
                        st.setInt(2, id);
                        st.addBatch();

                        st1.setInt(1, Integer.parseInt(arr[2]));
                        st1.setInt(2, id);
                        st.addBatch();
                    } catch (NumberFormatException e) {
                        continue;
                    }

                    count++;

                }
                st.executeBatch();
                conn.commit();
                batch++;
                System.out.println("running update:" + batch);
                // if(batch==11)break;
            }
            st.close();
            st1.close();
            sc.close();
            if (conn != null) {
                System.out.println("Connected to the database");
            }
        } catch (SQLException ex) {
            System.out.println("An error occurred. Maybe user/password is invalid");
            ex.printStackTrace();
        }
    }

    /**
     * loads movies in database
     * @throws IOException
     */
    public void Movies() throws IOException {

        InputStream gzinput = new GZIPInputStream(new FileInputStream(common + "title.basics.tsv.gz"));
        //file 1
        try {

            conn.setAutoCommit(false);
            Scanner sc = new Scanner(gzinput, "UTF-8");
            PreparedStatement st = null;
            PreparedStatement st1 = null;
            PreparedStatement st2 = null;
            st = conn.prepareStatement("INSERT INTO Movie(id,title,releaseYear,runtime) " +
                    "VALUES(?,?,?,?)");
            st1 = conn.prepareStatement("INSERT INTO Genre(id,name) VALUES(?,?)");
            st2 = conn.prepareStatement("INSERT INTO HasGenre(genreId,movieId) VALUES(?,?)");
            int batch = 1;
            sc.nextLine();
            while (sc.hasNextLine()) {
                int count = 0;

                while (count != 10000 && sc.hasNextLine()) {
                    String temp = sc.nextLine();
                    String[] arr = temp.split("\t");
                    if ((arr[1].equals("short") || arr[1].equals("tvShort") || arr[1].equals("tvMovie")
                            || arr[1].equals("movie"))) {
                        try {
                            int id = Integer.parseInt(arr[0].substring(2));

                            String genres[] = arr[8].split(",");
                            for (String str : genres) {
                                if (!map.containsKey(str)) {
                                    map.put(str, gcount++);
                                    st1.setInt(1, gcount - 1);
                                    st1.setString(2, str);
                                    st1.addBatch();

                                    st2.setInt(2, id);
                                    st2.setInt(1, gcount - 1);
                                    st2.addBatch();
                                } else {
                                    st2.setInt(2, id);
                                    st1.setInt(1, map.get(str));
                                    st2.addBatch();
                                }
                            }

                            st.setInt(1, id);
                            String title;
                            int release;
                            int runtime;
                            try {
                                title = arr[3];
                                st.setString(2, title);
                            } catch (Exception e) {
                                st.setNull(2, Types.NULL);
                            }
                            try {
                                release = Integer.parseInt(arr[5]);
                                st.setInt(3, release);
                            } catch (Exception e) {
                                st.setNull(3, Types.NULL);
                            }
                            try {
                                runtime = Integer.parseInt(arr[7]);
                                st.setInt(4, runtime);
                            } catch (Exception e) {
                                st.setNull(4, Types.NULL);
                            }
                            st.addBatch();
                        } catch (NumberFormatException e) {
                            continue;
                        }
                    }
                    count++;

                }
                st.executeBatch();
                st1.executeBatch();
                st2.executeBatch();
                conn.commit();
                batch++;
                System.out.println("running movies" +
                        ":" + batch);
                // if(batch==11)break;
            }
            st.close();
            st1.close();
            st2.close();
            sc.close();
            if (conn != null) {
                System.out.println("Connected to the database");
            }
        } catch (SQLException ex) {
            System.out.println("An error occurred. Maybe user/password is invalid");
            ex.printStackTrace();
        }
    }

    /**
     * maps proffesion in database
     * @throws IOException
     */
    public void mapping() throws IOException {

        InputStream gzinput = new GZIPInputStream(new FileInputStream(common + "title.principals.tsv.gz"));

        try {
            // conn = DriverManager.getConnection(databaseURL);
            conn.setAutoCommit(false);
            Scanner sc = new Scanner(gzinput, "UTF-8");
            sc.nextLine();
            PreparedStatement st1 = conn.prepareStatement("INSERT IGNORE INTO ActedIn(personId,movieId) SELECT " +
                    "Person.id, Movie.id FROM Person,Movie  WHERE Person.id=? AND Movie.id=?");
            PreparedStatement st2 = conn.prepareStatement("INSERT IGNORE INTO DirectedBy(personId,movieId) SELECT " +
                    "Person.id, Movie.id FROM Person,Movie  WHERE Person.id=? AND Movie.id=?");
            PreparedStatement st3 = conn.prepareStatement("INSERT IGNORE INTO ComposedBy(personId,movieId) SELECT " +
                    "Person.id, Movie.id FROM Person,Movie  WHERE Person.id=? AND Movie.id=?");
            PreparedStatement st4 = conn.prepareStatement("INSERT IGNORE INTO WrittenBy(personId,movieId) SELECT " +
                    "Person.id, Movie.id FROM Person,Movie  WHERE Person.id=? AND Movie.id=?");
            PreparedStatement st5 = conn.prepareStatement("INSERT IGNORE INTO EditedBy(personId,movieId) SELECT " +
                    "Person.id, Movie.id FROM Person,Movie  WHERE Person.id=? AND Movie.id=?");
            PreparedStatement st6 = conn.prepareStatement("INSERT IGNORE INTO ProducedBy(personId,movieId) SELECT " +
                    "Person.id, Movie.id FROM Person,Movie  WHERE Person.id=? AND Movie.id=?");
            int batch = 1;

            while (sc.hasNextLine()) {
                int count = 0;

                while (count != 20000 && sc.hasNextLine()) {
                    String temp = sc.nextLine();
                    String[] arr = temp.split("\t");

                    try {
                        String category = arr[3];
                        switch (category) {
                            case "self":
                                st1.setInt(1, Integer.parseInt(arr[2].substring(2)));
                                st1.setInt(2, Integer.parseInt(arr[0].substring(2)));
                                st1.addBatch();
                                break;

                            case "actor":
                                st1.setInt(1, Integer.parseInt(arr[2].substring(2)));
                                st1.setInt(2, Integer.parseInt(arr[0].substring(2)));
                                st1.addBatch();
                                break;

                            case "actress":
                                st1.setInt(1, Integer.parseInt(arr[2].substring(2)));
                                st1.setInt(2, Integer.parseInt(arr[0].substring(2)));
                                st1.addBatch();
                                break;

                            case "director":
                                st2.setInt(1, Integer.parseInt(arr[2].substring(2)));
                                st2.setInt(2, Integer.parseInt(arr[0].substring(2)));
                                st2.addBatch();
                                break;

                            case "editor":
                                st5.setInt(1, Integer.parseInt(arr[2].substring(2)));
                                st5.setInt(2, Integer.parseInt(arr[0].substring(2)));
                                st5.addBatch();
                                break;

                            case "composer":
                                st3.setInt(1, Integer.parseInt(arr[2].substring(2)));
                                st3.setInt(2, Integer.parseInt(arr[0].substring(2)));
                                st3.addBatch();
                                break;

                            case "writer":
                                st4.setInt(1, Integer.parseInt(arr[2].substring(2)));
                                st4.setInt(2, Integer.parseInt(arr[0].substring(2)));
                                st4.addBatch();
                                break;

                            case "producer":
                                st6.setInt(1, Integer.parseInt(arr[2].substring(2)));
                                st6.setInt(2, Integer.parseInt(arr[0].substring(2)));
                                st6.addBatch();

                            default:
                                break;

                        }

                    } catch (NumberFormatException e) {
                        continue;
                    }

                    count++;

                }
                st1.executeBatch();
                st2.executeBatch();
                st3.executeBatch();
                st4.executeBatch();
                st5.executeBatch();
                st6.executeBatch();
                conn.commit();
                batch++;
                System.out.println("running mapping:" + batch);
                //  if(batch==11)break;

            }

            sc.close();
            st1.close();
            st2.close();
            st3.close();
            st4.close();
            st5.close();
            st6.close();
            if (conn != null) {
                System.out.println("Connected to the database");
            }
        } catch (SQLException ex) {
            System.out.println("An error occurred. Maybe user/password is invalid");
            ex.printStackTrace();
        }
    }

    /**
    Takes user and database credentials, performs JDBC connection, and then performs the operations
    **/
    public static void main(String[] args) throws IOException, SQLException {
        // write your code here
        user = args[1];
        pass = args[2];
        common = args[3];
        databaseURL = args[0];
        conn = DriverManager.getConnection(databaseURL, user, pass);

        IMDBLoader im = new IMDBLoader();
        try {
            //create all the tables according to the schema
            im.createTables();
            // loads data to person table
            im.person();
            // loads data to movie table
            im.Movies();
            // updates the movie table with columns which were unfilled
            im.updateMovies();
            // performs the mapping of person with their profession and movies and stores in resp tables
            im.mapping();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }


    }
}


