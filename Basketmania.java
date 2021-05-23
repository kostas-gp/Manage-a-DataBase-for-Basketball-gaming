package Basketmania;

import java.sql.*;
import java.util.*;

public class DBoperations {

    private Connection conn = null;
    private final String serverURL = "127.0.0.1";
    private final String serverPort = "3306";
    private final String serverSID = "orcl";
    private final String username = "c##icsd12015";
    private final String password = "icsd12015";
//    private final String serverURL = "localhost";
//    private final String serverPort = "1521";
//    private final String serverSID = "XE";
//    private final String username = "basketmania";
//    private final String password = "basketmania";

    public DBoperations () throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
    }

    public void openConnection () throws SQLException {
        String connectionURL = "jdbc:oracle:thin:@" + serverURL + ":" + serverPort + ":" + serverSID;
        conn = DriverManager.getConnection(connectionURL, username, password);
        conn.setAutoCommit(false);
    }

    public void closeConnection () throws SQLException {
        conn.commit();
        conn.close();
    }

    public void insertData (String query) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate(query);
            conn.commit();
        }
    }

    public ArrayList<HashMap<String, String>> getResults (String query) throws SQLException {

        ArrayList<HashMap<String, String>> res;

        try (Statement statement = conn.createStatement()) {
            ResultSet results = statement.executeQuery(query);
            res = resultSetToArrayList(results);
        }
        return res;
    }

    public void updateTables (String query) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(query)) {
            ps.executeUpdate();
        }
    }

    public ArrayList<HashMap<String, String>> resultSetToArrayList (ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        ArrayList list = new ArrayList();
        while (rs.next()) {
            HashMap<String, String> row = new HashMap(columns);
            for (int i = 1; i <= columns; ++i) {
                String value = rs.getObject(i) != null ? rs.getObject(i).toString() : "null";
                row.put(md.getColumnName(i), value);
            }
            list.add(row);
        }
        return list;
    }

    public void storeGameData (String home, String visitors, String court, String attendance, String datetime,
                               ArrayList<HashMap<String, String>> playersStatistics) throws SQLException {
        String query;

        query = "SELECT "
                + "(SELECT ID AS ID_HOME FROM TEAMS WHERE NAME = '" + home + "') AS ID_HOME,"
                + "(SELECT ID AS ID_VISITORS FROM TEAMS WHERE NAME = '" + visitors + "') AS ID_VISITORS "
                + "FROM TEAMS WHERE ROWNUM = 1";

        ArrayList<HashMap<String, String>> results = getResults(query);

        if (!results.isEmpty()) {

            String id_home = results.get(0).get("ID_HOME");
            String id_visitors = results.get(0).get("ID_VISITORS");

            query = "INSERT INTO GAMES(ID_HOME,ID_VISITORS,ATTENDANCE,COURT,DATETIME) "
                    + "VALUES (" + id_home + ", " + id_visitors + ", " + attendance + ", "
                    + "'" + court + "'" + ", to_date('" + datetime + "','YYYY-MM-DD HH24:MI'))";

            insertData(query);

            query = "SELECT MAX(ID) AS ID_GAME FROM GAMES";

            results = getResults(query);

            if (!results.isEmpty()) {

                String game_id = results.get(0).get("ID_GAME");

                for (HashMap<String, String> row : playersStatistics) {
                    query = "SELECT ID FROM PLAYERS WHERE NAME = '" + row.get("NAME").replace(",", "")
                            + "' AND PNUM = " + row.get("PNUM");

                    results = getResults(query);

                    if (!results.isEmpty()) {
                        String player_id = results.get(0).get("ID");


                        query = "INSERT INTO GAMES_STATISTICS (GAME_ID, PLAYER_ID, TIME_PLAYED, POINTS, FG2_MADE, FG2_ATTEMTED, "
                                + "FG3_MADE, FG3_ATTEMTED, FT_MADE, FT_ATTEMTED, REBOUNDS_O, REBOUNDS_D, REBOUNDS_T, ASSISTS, STEALS, "
                                + "TURNOVERS, BLOCKS_FV, BLOCKS_AG, FOULS_CM, FOULS_RV, PIR) VALUES (" + game_id + ", " + player_id + ", '"
                                + row.get("TIME_PLAYED") + "', " + row.get("POINTS") + ", " + row.get("FG2_MADE")
                                + ", " + row.get("FG2_ATTEMTED") + ", " + row.get("FG3_MADE") + ", " + row.get("FG3_ATTEMTED") + ", "
                                + row.get("FT_MADE") + ", " + row.get("FT_ATTEMTED") + ", " + row.get("REBOUNDS_O") + ", "
                                + row.get("REBOUNDS_D") + ", " + row.get("REBOUNDS_T") + ", " + row.get("ASSISTS") + ", "
                                + row.get("STEALS") + ", " + row.get("TURNOVERS") + ", " + row.get("BLOCKS_FV") + ", "
                                + row.get("BLOCKS_AG") + ", " + row.get("FOULS_CM") + ", " + row.get("FOULS_RV") + ", "
                                + row.get("PIR") + ")";

                        System.out.println(query);
                        insertData(query);
                    }

                }
            }
        }
    }

    public ArrayList<HashMap<String, String>> getTeamsInfo (String condition) throws SQLException {
        String queryString;
        ArrayList<HashMap<String, String>> results = null;

        queryString = "SELECT NAME, SHORTNAME, LOGO "
                + "FROM TEAMS "
                + condition;

        return getResults(queryString);
    }

    public ArrayList<HashMap<String, String>> getTeamGamesStats (String Tshortname) throws SQLException {
        String queryString;
        ArrayList<HashMap<String, String>> temp = null, players = null, games = null, players2 = null, results = null;
        //ID KAI ONOMA OMADAS
        queryString = "SELECT ID, NAME "
                + "FROM TEAMS "
                + "WHERE SHORTNAME = '" + Tshortname + "'";

        temp = getResults(queryString);

        if (temp != null && !temp.isEmpty()) {

            HashMap<String, String> row_columns = temp.get(0);

            String TEAM_ID = row_columns.get("ID");
            String TEAM_NAME = row_columns.get("NAME");

            //PAIXTES OMADAS
            queryString = "SELECT "
                    + "PLAYER1_ID ,PLAYER2_ID ,PLAYER3_ID ,PLAYER4_ID ,PLAYER5_ID ,PLAYER6_ID ,PLAYER7_ID ,\n"
                    + "PLAYER8_ID ,PLAYER9_ID ,PLAYER10_ID ,PLAYER11_ID ,PLAYER12_ID ,PLAYER13_ID ,PLAYER14_ID "
                    + "FROM TEAMS_PLAYERS "
                    + "WHERE TEAM_ID = " + TEAM_ID;

            players = getResults(queryString);

            if (players != null && !players.isEmpty()) {

                //PAIXNIDIA OMADAS
                queryString = "SELECT * "
                        + "FROM GAMES "
                        + "WHERE ID_HOME = " + TEAM_ID
                        + " OR ID_VISITORS = " + TEAM_ID;

                games = getResults(queryString);

                if (games != null && !games.isEmpty()) {
                    results = new ArrayList();
                    Iterator<HashMap<String, String>> it = games.iterator();
                    while (it.hasNext()) {
                        row_columns = it.next();

                        String GAME_ID = row_columns.get("ID");
                        boolean WASHOME = row_columns.get("ID_HOME").equals(TEAM_ID);
                        String GAME_DATETIME = row_columns.get("DATETIME");
                        String GAME_COURT = row_columns.get("COURT");
                        String GAME_ATTENDANCE = row_columns.get("ATTENDANCE");

                        queryString = "SELECT SUM(POINTS) AS TOTAL_POINTS, "
                                + "sum(REBOUNDS_D + REBOUNDS_O + REBOUNDS_T) AS TOTAL_REBOUNDS, "
                                + "SUM(PIR) AS TOTAL_PIR "
                                + "FROM GAMES_STATISTICS "
                                + "WHERE GAME_ID = " + GAME_ID
                                + " AND PLAYER_ID IN ( " + players.get(0).values().toString().replaceAll("[\\[\\]]", "") + " )";

                        temp = getResults(queryString);

                        String TOTAL_POINTS_INFAVOR = temp.get(0).get("TOTAL_POINTS");
                        String TOTAL_REBOUNDS_INFAVOR = temp.get(0).get("TOTAL_REBOUNDS");
                        String TOTAL_PIR_INFAVOR = temp.get(0).get("TOTAL_PIR");

                        queryString = "SELECT " + (WASHOME ? "ID_VISITORS" : "ID_HOME") + " AS TEAM2_ID"
                                + " FROM GAMES "
                                + " WHERE ID = " + GAME_ID;

                        temp = getResults(queryString);

                        String TEAM2_ID = temp.get(0).get("TEAM2_ID");

                        queryString = "SELECT NAME "
                                + "FROM TEAMS "
                                + "WHERE ID = '" + TEAM2_ID + "'";

                        temp = getResults(queryString);

                        row_columns = temp.get(0);
                        String TEAM2_NAME = row_columns.get("NAME");

                        //PAIXTES ALLIS OMADAS
                        queryString = "SELECT "
                                + "PLAYER1_ID ,PLAYER2_ID ,PLAYER3_ID ,PLAYER4_ID ,PLAYER5_ID ,PLAYER6_ID ,PLAYER7_ID ,\n"
                                + "PLAYER8_ID ,PLAYER9_ID ,PLAYER10_ID ,PLAYER11_ID ,PLAYER12_ID ,PLAYER13_ID ,PLAYER14_ID "
                                + "FROM TEAMS_PLAYERS "
                                + "WHERE TEAM_id = " + TEAM2_ID;

                        players2 = getResults(queryString);

                        if (players2 != null && !players2.isEmpty()) {

                            queryString = "SELECT sum(points) AS TOTAL_POINTS, "
                                    + "sum(REBOUNDS_D + REBOUNDS_O + REBOUNDS_T) AS TOTAL_REBOUNDS, "
                                    + "SUM(PIR) AS TOTAL_PIR "
                                    + "FROM GAMES_STATISTICS "
                                    + "WHERE GAME_ID = " + GAME_ID
                                    + " AND PLAYER_ID IN ( "
                                    + players2.get(0).values().toString().replaceAll("[\\[\\]]", "") + " )";

                            temp = getResults(queryString);

                            if (temp != null && !temp.isEmpty()) {

                                String TOTAL_POINTS_AGAINST = temp.get(0).get("TOTAL_POINTS");
                                String TOTAL_REBOUNDS_AGAINST = temp.get(0).get("TOTAL_REBOUNDS");
                                String TOTAL_PIR_AGAINST = temp.get(0).get("TOTAL_PIR");

                                HashMap<String, String> temp2 = new HashMap();

                                temp2.put("GAME_DATETIME", GAME_DATETIME);
                                temp2.put("GAME_COURT", GAME_COURT);
                                temp2.put("GAME_ATTENDANCE", GAME_ATTENDANCE);
                                temp2.put("TEAM_NAME", TEAM_NAME);
                                temp2.put("TEAM2_NAME", TEAM2_NAME);
                                temp2.put("TOTAL_POINTS_INFAVOR", TOTAL_POINTS_INFAVOR);
                                temp2.put("TOTAL_REBOUNDS_INFAVOR", TOTAL_REBOUNDS_INFAVOR);
                                temp2.put("TOTAL_PIR_INFAVOR", TOTAL_PIR_INFAVOR);
                                temp2.put("TOTAL_POINTS_AGAINST", TOTAL_POINTS_AGAINST);
                                temp2.put("TOTAL_REBOUNDS_AGAINST", TOTAL_REBOUNDS_AGAINST);
                                temp2.put("TOTAL_PIR_AGAINST", TOTAL_PIR_AGAINST);
                                temp2.put("WASHOME", WASHOME ? "TRUE" : "FALSE");

                                results.add(temp2);
                            }
                        } else {
                            return players2;
                        }
                    }
                } else {
                    return games;
                }

            } else {
                return players;
            }
        }
        return results;
    }

    public ArrayList<HashMap<String, String>> getTeamPlayers (String Tshortname) throws SQLException {
        String queryString = "SELECT "
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER1_ID) AS PNAME1,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER1_ID) AS PPICTURE1,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER2_ID) AS PNAME2,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER2_ID) AS PPICTURE2,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER3_ID) AS PNAME3,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER3_ID) AS PPICTURE3,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER4_ID) AS PNAME4,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER4_ID) AS PPICTURE4,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER5_ID) AS PNAME5,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER5_ID) AS PPICTURE5,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER6_ID) AS PNAME6,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER6_ID) AS PPICTURE6,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER7_ID) AS PNAME7,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER7_ID) AS PPICTURE7,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER8_ID) AS PNAME8,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER8_ID) AS PPICTURE8,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER9_ID) AS PNAME9,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER9_ID) AS PPICTURE9,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER10_ID) AS PNAME10,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER10_ID) AS PPICTURE10,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER11_ID) AS PNAME11,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER11_ID) AS PPICTURE11,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER12_ID) AS PNAME12,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER12_ID) AS PPICTURE12,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER13_ID) AS PNAME13,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER13_ID) AS PPICTURE13,\n"
                + "(SELECT NAME FROM PLAYERS WHERE ID = TP.PLAYER14_ID) AS PNAME14,\n"
                + "(SELECT PICTURE FROM PLAYERS WHERE ID = TP.PLAYER14_ID) AS PPICTURE14 ";
        queryString += "FROM TEAMS T JOIN TEAMS_PLAYERS TP ON TP.TEAM_ID = T.ID ";
        queryString += "WHERE T.SHORTNAME = '" + Tshortname + "'";

        return getResults(queryString);
    }

    public ArrayList<HashMap<String, String>> getPlayerInfo (String Pname) throws SQLException {
        String queryString;
        ArrayList<HashMap<String, String>> results = null;

        queryString = "SELECT * ";
        queryString += "FROM PLAYERS ";
        queryString += "WHERE NAME = '" + Pname + "'";

        return getResults(queryString);
    }

    public ArrayList<HashMap<String, String>> getPlayerStatistics (String Pname) throws SQLException {
        String queryString;
        ArrayList<HashMap<String, String>> results = null, results2 = null, results3;

        queryString = "SELECT * ";
        queryString += "FROM (SELECT * FROM GAMES_STATISTICS "
                + "WHERE PLAYER_ID = (SELECT ID FROM PLAYERS WHERE NAME='" + Pname + "')) GS\n"
                + "LEFT JOIN\n"
                + "(SELECT * FROM GAMES) G\n"
                + "ON G.ID = GS.GAME_ID";

        results = getResults(queryString);

        if (results != null && !results.isEmpty()) {
            results3 = new ArrayList();
            for (HashMap<String, String> result : results) {
                queryString = "SELECT *";
                queryString += "FROM (SELECT NAME AS HOME FROM TEAMS WHERE ID=" + result.get("ID_HOME") + ")\n"
                        + "JOIN\n"
                        + "(SELECT  NAME AS VISITORS FROM TEAMS WHERE ID=" + result.get("ID_VISITORS") + ")\n"
                        + "ON 1=1";

                results2 = getResults(queryString);

                if (results2 != null && !results2.isEmpty()) {
                    result.put("VISITORS", results2.get(0).get("VISITORS"));
                    result.put("HOME", results2.get(0).get("HOME"));

                    results3.add(result);
                }
            }
        }
        return results;
    }

    public ArrayList<HashMap<String, String>> getTop50 () throws SQLException {
        String queryString;
        ArrayList<HashMap<String, String>> results = null;

        queryString = "SELECT * ";
        queryString += "FROM\n"
                + "(SELECT ROWNUM,SPIR,NAME,PICTURE\n"
                + "FROM\n"
                + "(SELECT  SUM(PIR) AS SPIR,PLAYER_ID \n"
                + "FROM GAMES_STATISTICS \n"
                + "GROUP BY PLAYER_ID) GS \n"
                + "JOIN\n"
                + "(SELECT * FROM PLAYERS) P\n"
                + "ON P.ID = GS.PLAYER_ID \n"
                + "ORDER BY SPIR DESC)\n"
                + "WHERE ROWNUM <= 50";

        return getResults(queryString);
    }

    public ArrayList<HashMap<String, String>> getTop5Agwnistikis (String periodosAgwnistikis, String kritirio) throws SQLException {
        String queryString;
        ArrayList<HashMap<String, String>> results = null;

        queryString = "SELECT * ";
        queryString += "FROM\n"
                + "(SELECT VALUE,NAME,PICTURE,POSITION\n"
                + "FROM\n"
                + "(SELECT  " + kritirio + " AS VALUE,PLAYER_ID \n"
                + "FROM GAMES_STATISTICS \n"
                + "WHERE GAME_ID IN \n"
                + "(SELECT ID FROM GAMES \n"
                + "WHERE DATETIME BETWEEN " + periodosAgwnistikis + ") \n"
                + "GROUP BY PLAYER_ID) GS \n"
                + "JOIN\n"
                + "(SELECT * FROM PLAYERS) P\n"
                + "ON P.ID = GS.PLAYER_ID ORDER BY VALUE DESC)\n"
                + "WHERE ROWNUM <= 5";

        return getResults(queryString);
    }

    public ArrayList<HashMap<String, String>> getGamesInfo () throws SQLException {
        String queryString;
        ArrayList<HashMap<String, String>> results = null;

        queryString = "SELECT GID, HOME, VISITORS, ATTENDANCE, DATETIME, COURT ";
        queryString += "FROM \n"
                + "(SELECT *\n"
                + "FROM \n"
                + "(SELECT ID AS GID, ID_HOME,ID_VISITORS,ATTENDANCE,DATETIME,COURT FROM GAMES\n"
                + "WHERE ID IN (SELECT ID \n"
                + "FROM GAMES)) G1\n"
                + "JOIN\n"
                + "(SELECT ID,NAME AS VISITORS FROM TEAMS) T1\n"
                + "ON T1.ID = G1.ID_VISITORS) G2\n"
                + "JOIN\n"
                + "(SELECT ID,NAME AS HOME FROM TEAMS) T2\n"
                + "ON T2.ID = G2.ID_HOME";

        return getResults(queryString);
    }

    public ArrayList<HashMap<String, String>> getTopByPosInGame (String position, String game_id, String plithos) throws
            SQLException {
        String queryString;
        ArrayList<HashMap<String, String>> results = null;

        queryString = "SELECT SPIR, NAME, PICTURE ";
        queryString += "FROM \n"
                + "(SELECT * FROM\n"
                + "(SELECT GAME_ID,SPIR,NAME,PICTURE\n"
                + "FROM\n"
                + "(SELECT  SUM(PIR) AS SPIR,PLAYER_ID,GAME_ID \n"
                + "FROM GAMES_STATISTICS \n"
                + "WHERE PLAYER_ID IN \n"
                + "(SELECT ID \n"
                + "FROM PLAYERS\n"
                + "WHERE POSITION = '" + position + "') \n"
                + "AND GAME_ID = " + game_id
                + " GROUP BY PLAYER_ID, GAME_ID) GS\n"
                + " JOIN\n"
                + "(SELECT * FROM PLAYERS) P\n"
                + "ON P.ID = GS.PLAYER_ID ORDER BY SPIR DESC) GA\n"
                + " JOIN\n"
                + "(SELECT ID,ID_HOME,ID_VISITORS FROM GAMES) G\n"
                + "ON G.ID = GA.GAME_ID) GAP\n"
                + "WHERE ROWNUM IN " + plithos;

        return getResults(queryString);
    }

}
