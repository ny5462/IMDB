/**
This program creates a couple of views from the imdb dataset,
First is a movie_g view , second is an actor_view

@author- Nikhil Yadav
**/

package GenerateMappings;
import java.sql.*;

public class GenerateMappings {
	
	public static void main(String args[]) throws SQLException {
		 long startTime = System.nanoTime();	
		 String url = args[0];
		 String user = args[1];
		 String pwd=args[2];
		 Connection con = null;

		//Statement stmt = null;
		con = DriverManager.getConnection(url, user, pwd);
		
		String sql="CREATE VIEW Movie_G AS SELECT id,title,year,'ActionDrama' as genre,null as boxoffice,color FROM  ActionDramaMovie union SELECT id,title,year,'Comedy' as genre ,boxoffice,null as color FROM ComedyMovie;";
		PreparedStatement st = con.prepareStatement(sql);
		
		st.executeUpdate(sql);
		 
		  sql="CREATE VIEW Actor_G AS SELECT * FROM  YoungActor;";
		  st = con.prepareStatement(sql);
		  
		  st.executeUpdate(sql);
		  sql="CREATE VIEW Movie_Actor_G AS SELECT * FROM Acted;";
		  st = con.prepareStatement(sql);
		  st.executeUpdate(sql);
		
		 long elapsedTime = System.nanoTime() - startTime;
	     
		 System.out.println("Total execution time in seconds: "
		     + elapsedTime/1000000000);
		
		
	}

}

