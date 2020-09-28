package CreateSources;
/**
 * This program creates 4 new sources from imdb_ibd
 * @author- Nikhil Yadav
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CreateSources {
    static Connection conn;

    /**
     * method to create tables or new sources
     * @throws SQLException
     */
    public void createTables() throws SQLException {
        PreparedStatement st=null;
        st=conn.prepareStatement("create table ComedyMovie(id INT NOT NULL, title VARCHAR(500), year INT,boxoffice VARCHAR(65))");
        st.executeUpdate();
        st=conn.prepareStatement("create table ActionDramaMovie(id INT NOT NULL, title VARCHAR(500),year INT, color VARCHAR(65))");
        st.executeUpdate();
        st=conn.prepareStatement("create table YoungActor(id INT NOT NULL, name VARCHAR(200), alive TINYINT, deathcause VARCHAR(200))");
        st.executeUpdate();
        st=conn.prepareStatement("create table Acted(actor INT ,movie INT)");
        st.executeUpdate();
        System.out.println("new sources created");
        st.close();
    }

    /**
     * fill in comedy movies with runtime >=75
     * @throws SQLException
     */
    public void Comedy() throws SQLException {
        PreparedStatement st =null;
        st=conn.prepareStatement("insert into ComedyMovie(id,title, year) select movie.id, movie.title," +
                " movie.releaseYear from movie join hasgenre on movie.id=hasgenre.movieId join genre on " +
                "hasgenre.genreId=genre.id where genre.name='Comedy' and movie.runtime>=75");
        st.executeUpdate();
        System.out.println("comedy with runtime >=75 inserted");
        st.close();
    }

    /**
     * fill in movies which have both action and drama genre
     * @throws SQLException
     */
    public void ActionDrama() throws SQLException {
        PreparedStatement st=null;
        st=conn.prepareStatement("insert into ActionDramaMovie(id,title, year) select movie.id, movie.title," +
                        "movie.releaseYear from movie join hasgenre on movie.id=hasgenre.movieId join genre on " +
                        "hasgenre.genreId=genre.id where genre.name in('Action','Drama') group by movie.id " +
                        "having count(movie.id)>1");
        st.executeUpdate();
        System.out.println("action drama genre filtered");
        st.close();
    }

    /**
     * fill in actors who are 30 or younger
     * @throws SQLException
     */
    public void YoungActor() throws SQLException{
        PreparedStatement st=null;
        st=conn.prepareStatement("insert into YoungActor(id,name,alive) select distinct person.id, person.name," +
                " case when person.deathYear is null then 1 else 0 end as alive from person join actedin on person.id=actedin.personId" +
                " where person.birthYear>=1990");
        st.executeUpdate();
        System.out.println(" young actor checked");
        st.close();
    }

    /**
     * fill in mapping of young actors and selected movies
     * @throws SQLException
     */
    public void Acted() throws SQLException{
        PreparedStatement st=null;
        st=conn.prepareStatement("insert ignore  into Acted(actor, movie) select actedin.personId, actedin.movieId " +
                "from actedin join person on actedin.personId=person.id join movie on actedin.movieId=movie.id  where " +
                "actedin.personId in(select id from YoungActor) and (actedin.movieId in(select id from ComedyMovie) or " +
                "actedin.movieId in (select id from ActionDramaMovie))");
        st.executeUpdate();
        System.out.println("young actor with s1,s2 mapping done");
        st.close();
    }

    public static void main(String[] args) throws SQLException {
        conn =null;
        String dbURL = args[0];
        String user = args[1];
        String pass = args[2];
        conn = DriverManager.getConnection(dbURL, user, pass);

        CreateSources cs=new CreateSources();
        cs.createTables();
        cs.Comedy();
        cs.ActionDrama();
        cs.YoungActor();
        cs.Acted();

    }
}
