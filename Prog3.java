/*
 * Prog3.java - The purpose of this program is to use the data
 * gathered from Arizona schools' kwiedeman.aims scores, and to use this in
 * a database to answer user queries. The user has 4 options as 
 * to what query the would like to select.
 * 
 * To compile and execute this program on lectura:
 *
 *	ensure that first, if you do not have the oracle JDBC driver included in your
 *  Java classpath, add it by entering the below command to lectura first.
 *  
 *         export CLASSPATH=/opt/oracle/product/10.2.0/client/jdbc/lib/ojdbc14.jar:${CLASSPATH}
 *
 *     (or whatever shell variable set-up you need to perform to add the
 *     JAR file to your Java CLASSPATH)
 *
 *  To then compile the file:
 *
 *         javac Prog3.java
 *
 *   Finally, you can run the program (assuming you have an oracle 
 *   username and password) with:
 *
 *         java Prog3 <oracle username> <oracle password>
 *
 * Author:  Kevin Wiedeman
 * Turned in on: November 11th, 2015
 * Credit also goes to Doctor Lester McCann for providing the base
 * example code for this program, complete with magic access spells, etcetera.
 */

import java.io.*;
import java.sql.*;                 // For access to the SQL interaction methods
import java.util.Scanner;

public class Prog3
{
    public static void main (String [] args)
    {

        final String oracleURL =   // Magic lectura -> aloe access spell
                        "jdbc:oracle:thin:@aloe.cs.arizona.edu:1521:oracle";

        String query = "";

        String username = null,    // Oracle DBMS username
               password = null;    // Oracle DBMS password


        if (args.length == 2) {    // get username/password from command line args
            username = args[0];
            password = args[1];
        } else {
            System.out.println("\nUsage:  java JDBC <username> <password>\n"
                             + "    where <username> is your Oracle DBMS"
                             + " username,\n    and <password> is your Oracle"
                             + " password (not your system password).\n");
            System.exit(-1);
        }


            // load the (Oracle) JDBC driver by initializing its base
            // class, 'oracle.jdbc.OracleDriver'.

        try {

                Class.forName("oracle.jdbc.OracleDriver");

        } catch (ClassNotFoundException e) {

                System.err.println("*** ClassNotFoundException:  "
                    + "Error loading Oracle JDBC driver.  \n"
                    + "\tPerhaps the driver is not on the Classpath?");
                System.exit(-1);

        }


            // make and return a database connection to the user's
            // Oracle database

        Connection dbconn = null;

        try {
                dbconn = DriverManager.getConnection
                               (oracleURL,username,password);

        } catch (SQLException e) {

                System.err.println("*** SQLException:  "
                    + "Could not open JDBC connection.");
                System.err.println("\tMessage:   " + e.getMessage());
                System.err.println("\tSQLState:  " + e.getSQLState());
                System.err.println("\tErrorCode: " + e.getErrorCode());
                System.exit(-1);

        }


            // Send the query to the DBMS, and get and display the results

        Statement stmt = null;
        ResultSet answer = null;

        Scanner scanner = new Scanner(System.in);
        System.out.println("Welcome!\nYou may select one of four queries to search the Arizona kwiedeman.aims database for information you are interested in."
        		+ "\nPlease type the number of the query and press ENTER.\n\n"
        		+ "1: Number of middle schools listed in 2013\n"
        		+ "2: The names and LEA names of schools that had strictly decreasing scores of, 'Falls far below' in the math category from 2010-2014\n"
        		+ "3: The names of the top 5 schools throughout 2010-2014 that had the highest average scale scores\n"
        		+ "4: The names of the top 10 school that got exceeding scores in each kwiedeman.aims category since a user-specified year in a user-specified county\n"
        		+ "or type 'exit' to exit.");
        
        String input="";
        while(scanner.hasNextLine()){
        	input=scanner.nextLine();
        	if(input.equals("1"))						// if the user inputs 1, execute query 1
        		query1(query, answer, dbconn, stmt);
        	else if(input.equals("2")) 					// if the user inputs 2, execute query 2
        		query2(query, answer, dbconn, stmt);
        	else if(input.equals("3"))					// if the user inputs 3, execute query 3
        		query3(query, answer, dbconn, stmt);
        	else if(input.equals("4"))					// if the user inputs 4, execute query 4
        		query4(scanner, query, answer, dbconn, stmt);
        	else if(input.equals("exit")){
        		try {
					dbconn.close();
				} catch (SQLException e) {
				}
        		return;
        	}
        	else
        		System.out.println("Sorry, that input is not valid. Please try again.");	// If incorrect input is received, the user may try different input.
        	System.out.println("input:"+input);
        
        }

    }

	private static void query1(String query, ResultSet answer, Connection dbconn, Statement stmt) {
		String s=("SELECT count (distinct School_Name) from kwiedeman.aims2013");
		query=s;
		try {
        	if(!query.equals("Failed")){
            stmt = dbconn.createStatement();
            answer = stmt.executeQuery(query);
        	}
        	else System.out.println("Query failed.");

            if (answer != null) {

                System.out.println("\nThe results of the query [" + query 
                                 + "] are:\n");

                    // Get the data about the query result to learn
                    // the attribute names and use them as column headers

                ResultSetMetaData answermetadata = answer.getMetaData();

                for (int i = 1; i <= answermetadata.getColumnCount(); i++) {
                    System.out.print(answermetadata.getColumnName(i) + "\t");
                }
                System.out.println();

                    // Use next() to advance cursor through the result
                    // tuples and print their attribute values

                while (answer.next()) {
                    System.out.println(answer.getInt(1));
                }
            }
            System.out.println();

                // Shut down the connection to the DBMS.

        } catch (SQLException e) {

                System.err.println("*** SQLException:  "
                    + "Could not fetch query results.");
                System.err.println("\tMessage:   " + e.getMessage());
                System.err.println("\tSQLState:  " + e.getSQLState());
                System.err.println("\tErrorCode: " + e.getErrorCode());
                System.exit(-1);
        }
	}

	private static void query2(String query, ResultSet answer, Connection dbconn, Statement stmt) {
	String s=("SELECT distinct kwiedeman.aims2012.School_Name, kwiedeman.aims2012.LEA_Name FROM kwiedeman.aims2012" 
		+" INNER JOIN kwiedeman.aims2013 ON kwiedeman.aims2012.School_Name = kwiedeman.aims2013.School_Name" 
		+" INNER JOIN kwiedeman.aims2014 ON kwiedeman.aims2012.School_Name = kwiedeman.aims2014.School_Name" 
		+" INNER JOIN kwiedeman.aims2011 ON kwiedeman.aims2012.School_Name = kwiedeman.aims2011.School_Name" 
		+" INNER JOIN kwiedeman.aims2010 ON kwiedeman.aims2012.School_Name = kwiedeman.aims2010.School_Name" 
		+" where kwiedeman.aims2010.Math_Falls > kwiedeman.aims2011.Math_Falls" 
		+" and kwiedeman.aims2011.Math_Falls > kwiedeman.aims2012.Math_Falls" 
		+" and kwiedeman.aims2012.Math_Falls > kwiedeman.aims2013.Math_Falls" 
		+" and kwiedeman.aims2013.Math_Falls > kwiedeman.aims2014.Math_Falls"
		+" and kwiedeman.aims2010.school_name = kwiedeman.aims2011.school_name"
		+" and kwiedeman.aims2011.school_name = kwiedeman.aims2012.school_name"
		+" and kwiedeman.aims2012.school_name = kwiedeman.aims2013.school_name"
		+" and kwiedeman.aims2013.school_name = kwiedeman.aims2014.school_name");
	query=s;
	try {
    	if(!query.equals("Failed")){
        stmt = dbconn.createStatement();
        answer = stmt.executeQuery(query);
    	}
    	else System.out.println("Query failed.");

        if (answer != null) {

            System.out.println("\nThe results of the query [" + query 
                             + "] are:\n");

                // Get the data about the query result to learn
                // the attribute names and use them as column headers

            ResultSetMetaData answermetadata = answer.getMetaData();

            for (int i = 1; i <= answermetadata.getColumnCount(); i++) {
                System.out.print(answermetadata.getColumnName(i) + "\t");
            }
            System.out.println();

                // Use next() to advance cursor through the result
                // tuples and print their attribute values

            while (answer.next()) {
                System.out.println(answer.getString("School_Name") + "\t"
                    + answer.getString("LEA_Name"));
            }
        }
        System.out.println();

            // Shut down the connection to the DBMS.


    } catch (SQLException e) {

            System.err.println("*** SQLException:  "
                + "Could not fetch query results.");
            System.err.println("\tMessage:   " + e.getMessage());
            System.err.println("\tSQLState:  " + e.getSQLState());
            System.err.println("\tErrorCode: " + e.getErrorCode());
            System.exit(-1);

    }
	}

	private static void query3(String query, ResultSet answer, Connection dbconn, Statement stmt) {
		String s=("select school_name from ("
				+ "select * from ("
				+ " select school_name, math_mean_scale_score"
				+ " from kwiedeman.aims2010"
				+ " Union all"
				+ " select school_name, math_mean_scale_score"
				+ " from kwiedeman.aims2011"
				+ " Union all"
				+ " select school_name, math_mean_scale_score"
				+ " from kwiedeman.aims2012"
				+ " Union all"
				+ " select school_name, math_mean_scale_score"
				+ " from kwiedeman.aims2013"
				+ " Union all"
				+ " select school_name, math_mean_scale_score"
				+ " from kwiedeman.aims2014"
				+ ")"
				+ " order by math_mean_scale_score desc)"
				+ " where rownum < 6");
		query=s;
		 ResultSetMetaData answermetadata;
		try {
            stmt = dbconn.createStatement();
            answer = stmt.executeQuery(query);

            if (answer != null) {

                System.out.println("\nThe results of the query [" + query 
                                 + "] are:\n");
                System.out.println("\nTop 5 Math:\n");

                    // Get the data about the query result to learn
                    // the attribute names and use them as column headers

                answermetadata = answer.getMetaData();

                for (int i = 1; i <= answermetadata.getColumnCount(); i++) {
                    System.out.print(answermetadata.getColumnName(i) + "\t");
                }
                System.out.println();

                    // Use next() to advance cursor through the result
                    // tuples and print their attribute values

                while (answer.next()) {
                    System.out.println(answer.getString("school_name"));
                }
            }
                query=("select school_name from ("
        				+ "select * from ("
        				+ " select school_name, reading_mean_scale_score"
        				+ " from kwiedeman.aims2010"
        				+ " Union all"
        				+ " select school_name, reading_mean_scale_score"
        				+ " from kwiedeman.aims2011"
        				+ " Union all"
        				+ " select school_name, reading_mean_scale_score"
        				+ " from kwiedeman.aims2012"
        				+ " Union all"
        				+ " select school_name, reading_mean_scale_score"
        				+ " from kwiedeman.aims2013"
        				+ " Union all"
        				+ " select school_name, reading_mean_scale_score"
        				+ " from kwiedeman.aims2014"
        				+ ")"
        				+ " order by reading_mean_scale_score desc)"
        				+ " where rownum < 6");
                stmt = dbconn.createStatement();
                answer = stmt.executeQuery(query);

                if (answer != null) {

                    System.out.println("\nTop 5 Reading:\n");

                        // Get the data about the query result to learn
                        // the attribute names and use them as column headers

                    answermetadata = answer.getMetaData();

                    for (int i = 1; i <= answermetadata.getColumnCount(); i++) {
                        System.out.print(answermetadata.getColumnName(i) + "\t");
                    }
                    System.out.println();

                        // Use next() to advance cursor through the result
                        // tuples and print their attribute values

                    while (answer.next()) {
                        System.out.println(answer.getString("school_name"));
                    }
                }
                    query=("select school_name from ("
            				+ "select * from ("
            				+ " select school_name, science_mean_scale_score"
            				+ " from kwiedeman.aims2010"
            				+ " Union all"
            				+ " select school_name, science_mean_scale_score"
            				+ " from kwiedeman.aims2011"
            				+ " Union all"
            				+ " select school_name, science_mean_scale_score"
            				+ " from kwiedeman.aims2012"
            				+ " Union all"
            				+ " select school_name, science_mean_scale_score"
            				+ " from kwiedeman.aims2013"
            				+ " Union all"
            				+ " select school_name, science_mean_scale_score"
            				+ " from kwiedeman.aims2014"
            				+ ")"
            				+ " order by science_mean_scale_score desc)"
            				+ " where rownum < 6");
                    stmt = dbconn.createStatement();
                    answer = stmt.executeQuery(query);

                    if (answer != null) {

                    	  System.out.println("\nTop 5 Science:\n");

                            // Get the data about the query result to learn
                            // the attribute names and use them as column headers

                        answermetadata = answer.getMetaData();

                        for (int i = 1; i <= answermetadata.getColumnCount(); i++) {
                            System.out.print(answermetadata.getColumnName(i) + "\t");
                        }
                        System.out.println();

                            // Use next() to advance cursor through the result
                            // tuples and print their attribute values

                        while (answer.next()) {
                            System.out.println(answer.getString("school_name"));
                        }
                    }
                        query=("select school_name from ("
                				+ "select * from ("
                				+ " select school_name, writing_mean_scale_score"
                				+ " from kwiedeman.aims2010"
                				+ " Union all"
                				+ " select school_name, writing_mean_scale_score"
                				+ " from kwiedeman.aims2011"
                				+ " Union all"
                				+ " select school_name, writing_mean_scale_score"
                				+ " from kwiedeman.aims2012"
                				+ " Union all"
                				+ " select school_name, writing_mean_scale_score"
                				+ " from kwiedeman.aims2013"
                				+ " Union all"
                				+ " select school_name, writing_mean_scale_score"
                				+ " from kwiedeman.aims2014"
                				+ ")"
                				+ " order by writing_mean_scale_score desc)"
                				+ " where rownum < 6");
                        stmt = dbconn.createStatement();
                        answer = stmt.executeQuery(query);
                        
                        if (answer != null) {

                        	  System.out.println("\nTop 5 Writing\n");

                                // Get the data about the query result to learn
                                // the attribute names and use them as column headers

                            answermetadata = answer.getMetaData();

                            for (int i = 1; i <= answermetadata.getColumnCount(); i++) {
                                System.out.print(answermetadata.getColumnName(i) + "\t");
                            }
                            System.out.println();

                                // Use next() to advance cursor through the result
                                // tuples and print their attribute values

                            while (answer.next()) {
                                System.out.println(answer.getString("school_name"));
                            }

            }
            System.out.println();

        } catch (SQLException e) {

                System.err.println("*** SQLException:  "
                    + "Could not fetch query results.");
                System.err.println("\tMessage:   " + e.getMessage());
                System.err.println("\tSQLState:  " + e.getSQLState());
                System.err.println("\tErrorCode: " + e.getErrorCode());
                System.exit(-1);

        }
	}

	private static void query4(Scanner scanner, String query, ResultSet answer, Connection dbconn, Statement stmt) {
		int year;
		String county="";
		System.out.println("This query requires user input. It gets the percentages of \n"					// This is a somewhat more complex query, and so 
				+ "students exceeding in each section of the kwiedeman.aims test since a user-specified year \n"		// extra explanation as to what the query does is
				+ "in a user-specified county. Use this to find the best school for your child!");			// given to the user here. The user is then prompted
		System.out.println("Since year:");																	// for the first year from which they wish to see 
		year=Integer.parseInt(scanner.nextLine());															// results, and they are then prompted for a county
		System.out.println("In county:");																	// in which to search.
		county=scanner.nextLine();
		if(year<2010 || year>2014){
			System.out.println("Incorrect input. Please try again.");										// There is a check in place to ensure the user picks
			return;																							// a valid year, but there is no check to ensure the
		}																									// county specified is in the tables.
		String s="";
		for(;year<2015;year++){
			System.out.println("In year "+year+":");
			s=("select school_name, writing_percent_exceeds from ("
				+ "select * from kwiedeman.aims"+year
				+ " over order by writing_percent_exceeds desc)"
				+ " where county='"+county+"'"
				+ " and writing_percent_exceeds!=0"
				+ " and rownum < 11");
		query=s;
		try {
        	if(!query.equals("Failed")){
            stmt = dbconn.createStatement();
            answer = stmt.executeQuery(query);
        	}
        	else System.out.println("Query failed.");

            if (answer != null) {

                System.out.println("\nThe results of the query [" + query 
                                 + "] are:\n");

                    // Get the data about the query result to learn
                    // the attribute names and use them as column headers

                ResultSetMetaData answermetadata = answer.getMetaData();

                for (int i = 1; i <= answermetadata.getColumnCount(); i++) {
                    System.out.print(answermetadata.getColumnName(i) + "\t");
                }
                System.out.println();

                    // Use next() to advance cursor through the result
                    // tuples and print their attribute values

                while (answer.next()) {
                    System.out.println(answer.getString("school_name") + "\t"
                        + answer.getInt("writing_percent_exceeds"));
                }
            }
            System.out.println();



        } catch (SQLException e) {

                System.err.println("*** SQLException:  "
                    + "Could not fetch query results.");
                System.err.println("\tMessage:   " + e.getMessage());
                System.err.println("\tSQLState:  " + e.getSQLState());
                System.err.println("\tErrorCode: " + e.getErrorCode());
                System.exit(-1);

        }	
		
		
		s=("select school_name, reading_percent_exceeds from ("
				+ "select * from kwiedeman.aims"+year
				+ " over order by reading_percent_exceeds desc)"
				+ " where county='"+county+"'"
				+ " and reading_percent_exceeds!=0"
				+ " and rownum < 11");
		query=s;
		try {
        	if(!query.equals("Failed")){
            stmt = dbconn.createStatement();
            answer = stmt.executeQuery(query);
        	}
        	else System.out.println("Query failed.");

            if (answer != null) {


                    // Get the data about the query result to learn
                    // the attribute names and use them as column headers

                ResultSetMetaData answermetadata = answer.getMetaData();

                for (int i = 1; i <= answermetadata.getColumnCount(); i++) {
                    System.out.print(answermetadata.getColumnName(i) + "\t");
                }
                System.out.println();

                    // Use next() to advance cursor through the result
                    // tuples and print their attribute values

                while (answer.next()) {
                    System.out.println(answer.getString("school_name") + "\t"
                        + answer.getInt("reading_percent_exceeds"));
                }
            }
            System.out.println();


        } catch (SQLException e) {

                System.err.println("*** SQLException:  "
                    + "Could not fetch query results.");
                System.err.println("\tMessage:   " + e.getMessage());
                System.err.println("\tSQLState:  " + e.getSQLState());
                System.err.println("\tErrorCode: " + e.getErrorCode());
                System.exit(-1);

        }
		
		
		s=("select school_name, science_percent_exceeds from ("
				+ "select * from kwiedeman.aims"+year
				+ " over order by science_percent_exceeds desc)"
				+ " where county='"+county+"'"
				+ " and science_percent_exceeds!=0"
				+ " and rownum < 11");
		query=s;
		try {
        	if(!query.equals("Failed")){
            stmt = dbconn.createStatement();
            answer = stmt.executeQuery(query);
        	}
        	else System.out.println("Query failed.");

            if (answer != null) {


                    // Get the data about the query result to learn
                    // the attribute names and use them as column headers

                ResultSetMetaData answermetadata = answer.getMetaData();

                for (int i = 1; i <= answermetadata.getColumnCount(); i++) {
                    System.out.print(answermetadata.getColumnName(i) + "\t");
                }
                System.out.println();

                    // Use next() to advance cursor through relations and print their selected values

                while (answer.next()) {
                    System.out.println(answer.getString("school_name") + "\t"
                        + answer.getInt("science_percent_exceeds"));
                }
            }
            System.out.println();



        } catch (SQLException e) {

                System.err.println("*** SQLException:  "
                    + "Could not fetch query results.");
                System.err.println("\tMessage:   " + e.getMessage());
                System.err.println("\tSQLState:  " + e.getSQLState());
                System.err.println("\tErrorCode: " + e.getErrorCode());
                System.exit(-1);

        }	

		
		s=("select school_name, math_percent_exceeds from ("
				+ "select * from kwiedeman.aims"+year
				+ " over order by math_percent_exceeds desc)"
				+ " where county='"+county+"'"
				+ " and math_percent_exceeds!=0"
				+ " and rownum < 11");query=s;
		try {
        	if(!query.equals("Failed")){
            stmt = dbconn.createStatement();
            answer = stmt.executeQuery(query);
        	}
        	else System.out.println("Query failed.");

            if (answer != null) {


                    // Get the data about the query result to learn
                    // the attribute names and use them as column headers

                ResultSetMetaData answermetadata = answer.getMetaData();

                for (int i = 1; i <= answermetadata.getColumnCount(); i++) {
                    System.out.print(answermetadata.getColumnName(i) + "\t");
                }
                System.out.println();

                    // Use next() to advance cursor through the result
                    // tuples and print their attribute values

                while (answer.next()) {
                    System.out.println(answer.getString("school_name") + "\t"
                        + answer.getInt("math_percent_exceeds"));
                }
            }
            System.out.println();


        } catch (SQLException e) {

                System.err.println("*** SQLException:  "
                    + "Could not fetch query results.");
                System.err.println("\tMessage:   " + e.getMessage());
                System.err.println("\tSQLState:  " + e.getSQLState());
                System.err.println("\tErrorCode: " + e.getErrorCode());
                System.exit(-1);

        }
        }	
	}
		
	
}
