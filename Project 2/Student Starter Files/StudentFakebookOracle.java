package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }
    
    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Birthed, Month_of_Birth " +         // select birth months and number of uses with that birth month
                "FROM " + UsersTable + " " +                            // from all users
                "WHERE Month_of_Birth IS NOT NULL " +                   // for which a birth month is available
                "GROUP BY Month_of_Birth " +                            // group into buckets by birth month
                "ORDER BY Birthed DESC, Month_of_Birth ASC");           // sort by users born in that month, descending; break ties by birth month
            
            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) {                       // step through result rows/records one by one
                if (rst.isFirst()) {                   // if first record
                    mostMonth = rst.getInt(2);         //   it is the month with the most
                }
                if (rst.isLast()) {                    // if last record
                    leastMonth = rst.getInt(2);        //   it is the month with the least
                }
                total += rst.getInt(1);                // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);
            
            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + mostMonth + " " +             // born in the most popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + leastMonth + " " +            // born in the least popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }
    
    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */

            FirstNameInfo info = new FirstNameInfo();

            String maxQuery = "SELECT DISTINCT First_Name " +
                              "FROM " + UsersTable + " " +
                              "WHERE LENGTH(First_Name) = (SELECT MAX(LENGTH(U1.First_Name)) " +
                              "FROM " + UsersTable + " U1) " +
                              "ORDER BY First_Name ASC";
            ResultSet max = stmt.executeQuery(maxQuery);
            while (max.next()) {
                info.addLongName(max.getString(1));
            }
            max.close();

            String minQuery = "SELECT DISTINCT First_Name " +
                              "FROM " + UsersTable + " " +
                              "WHERE LENGTH(First_Name) = (SELECT MIN(LENGTH(U1.First_Name)) " +
                              "FROM " + UsersTable + " U1) " +
                              "ORDER BY First_Name ASC";
            ResultSet min = stmt.executeQuery(minQuery);
            while (min.next()) {
                info.addShortName(min.getString(1));
            }
            min.close();

            String commonQuery = "SELECT First_Name, COUNT(User_ID) " +
                                 "FROM " + UsersTable + " " +
                                 "GROUP BY First_Name " +
                                 "HAVING COUNT(User_ID) = (SELECT MAX(COUNT(U1.User_ID)) " +
                                 "FROM " + UsersTable + " U1 " +
                                 "GROUP BY U1.First_Name) " +
                                 "ORDER BY First_Name ASC";
            ResultSet common = stmt.executeQuery(commonQuery);
            while (common.next()) {
                info.addCommonName(common.getString(1));
                info.setCommonNameCount(common.getLong(2));
            }
            common.close();

            stmt.close();
            return info;                // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }
    
    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */

            String lonely =  "CREATE VIEW Lonely AS " +
                             "(SELECT U1.USER_ID " +
                             "FROM " + UsersTable + " U1 " +
                             "MINUS " + 
                             "SELECT F1.USER1_ID " +
                             "FROM " + FriendsTable + " F1) " +
                             "INTERSECT " +
                             "(SELECT U2.USER_ID " +
                             "FROM " + UsersTable + " U2 " +
                             "MINUS " +
                             "SELECT F2.USER2_ID " +
                             "FROM " + FriendsTable + " F2)";
            stmt.executeQuery(lonely);

            String lonelyID = "SELECT U.User_ID, U.First_Name, U.Last_Name " +
                              "FROM " + UsersTable + " U, Lonely L " +
                              "WHERE U.USER_ID = L.USER_ID " +
                              "ORDER BY U.USER_ID ASC";
            ResultSet id = stmt.executeQuery(lonelyID);
            while (id.next()) {
                UserInfo u = new UserInfo(id.getLong(1), id.getString(2), id.getString(3));
                results.add(u);
            }


            String dropLonely = "DROP VIEW Lonely";
            stmt.executeQuery(dropLonely);

            id.close();
            stmt.close();

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */
            String liveAway = "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                              "FROM " + UsersTable + " U " +
                              "WHERE U.USER_ID IN " +
                              "(SELECT UC.USER_ID " +
                              "FROM " + CurrentCitiesTable + " UC " +
                              "JOIN " + HometownCitiesTable + " UH " +
                              "ON UC.USER_ID = UH.USER_ID " +
                              "WHERE UC.CURRENT_CITY_ID <> UH.HOMETOWN_CITY_ID) " +
                              "ORDER BY U.USER_ID ASC";
            ResultSet la = stmt.executeQuery(liveAway);
            while (la.next()) {
                UserInfo u = new UserInfo(la.getLong(1), la.getString(2), la.getString(3));
                results.add(u);
            }

            la.close();
            stmt.close();

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */

            String topTag = "CREATE VIEW Top_Tagged AS " +
                            "SELECT TAG_PHOTO_ID, CT " +
                            "FROM (SELECT TAG_PHOTO_ID, COUNT(TAG_SUBJECT_ID) AS CT " +
                            "FROM " + TagsTable +
                            " GROUP BY TAG_PHOTO_ID " +
                            "ORDER BY COUNT(TAG_SUBJECT_ID) DESC, TAG_PHOTO_ID ASC) " +
                            "WHERE ROWNUM <= " + num;
            stmt.executeQuery(topTag);


            String photo = "SELECT TT.TAG_PHOTO_ID, P.PHOTO_LINK, P.ALBUM_ID, A.ALBUM_NAME, U.USER_ID, U.FIRST_NAME, U.LAST_NAME, TT.CT " +
                           "FROM Top_Tagged TT " +
                           "LEFT JOIN " + TagsTable + " T ON T.TAG_PHOTO_ID = TT.TAG_PHOTO_ID " +
                           "LEFT JOIN " + UsersTable + " U ON T.TAG_SUBJECT_ID = U.USER_ID " +
                           "LEFT JOIN " + PhotosTable + " P ON T.TAG_PHOTO_ID = P.PHOTO_ID " +
                           "LEFT JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " +
                           "ORDER BY TT.CT DESC, T.TAG_PHOTO_ID ASC, U.USER_ID ASC";
            ResultSet ph = stmt.executeQuery(photo);


            while (ph.next()) {
                PhotoInfo p = new PhotoInfo(ph.getLong(1), ph.getLong(3), ph.getString(2), ph.getString(4));
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                UserInfo u = new UserInfo(ph.getLong(5), ph.getString(6), ph.getString(7));
                tp.addTaggedUser(u);

                for (int i = 0; i < ph.getInt(8) - 1; i ++) {
                    ph.next();
                    UserInfo u1 = new UserInfo(ph.getLong(5), ph.getString(6), ph.getString(7));
                    tp.addTaggedUser(u1);
                }
                results.add(tp);
            }
            
            String drop_topTag = "DROP VIEW Top_Tagged";
            stmt.executeQuery(drop_topTag);

            ph.close();
            stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */


            String tagid = "CREATE VIEW TAG_ID AS " +
                           "SELECT T1.TAG_SUBJECT_ID AS user_1, T2.TAG_SUBJECT_ID AS user_2, T1.TAG_PHOTO_ID " +
                           "FROM " + TagsTable + " T1, " + TagsTable + " T2 " +
                           "WHERE T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID AND T1.TAG_SUBJECT_ID < T2.TAG_SUBJECT_ID";
            stmt.executeQuery(tagid);

            String potenfri = "CREATE VIEW POT_FRI AS " +
                              "SELECT * " +
                              "FROM " +
                              "(SELECT U1.USER_ID AS user_1, U2.User_ID AS user_2, COUNT(*) AS CT " +
                              "FROM " + UsersTable + " U1, " + UsersTable + " U2, TAG_ID T " +
                              "WHERE U1.GENDER IS NOT NULL AND U2.GENDER IS NOT NULL AND U1.YEAR_OF_BIRTH IS NOT NULL AND U2.YEAR_OF_BIRTH IS NOT NULL AND " +
                              "U1.GENDER = U2.GENDER AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) <= " + yearDiff + " AND " +
                              "T.user_1 = U1.USER_ID AND T.user_2 = U2.USER_ID AND " +
                              "NOT EXISTS (SELECT * " +
                              "FROM " + FriendsTable + " F " +
                              "WHERE U1.USER_ID = F.USER1_ID AND U2.USER_ID = F.USER2_ID) " +
                              "GROUP BY U1.USER_ID, U2.USER_ID " +
                              "ORDER BY COUNT(*) DESC, U1.USER_ID ASC, U2.USER_ID ASC) " +
                              "WHERE ROWNUM <= " + num;
            stmt.executeQuery(potenfri);

            String outdata = "SELECT PF.user_1, U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, PF.user_2, U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH, P.PHOTO_ID, P.PHOTO_LINK, A.ALBUM_ID, A.ALBUM_NAME, PF.CT " +
                             "FROM POT_FRI PF " +
                             "LEFT JOIN TAG_ID T ON PF.user_1 = T.user_1 AND PF.user_2 = T.user_2 " +
                             "LEFT JOIN " + UsersTable + " U1 on PF.user_1 = U1.USER_ID " +
                             "LEFT JOIN " + UsersTable + " U2 on PF.user_2 = U2.USER_ID " +
                             "LEFT JOIN " + PhotosTable + " P ON P.PHOTO_ID = T.TAG_PHOTO_ID " +
                             "LEFT JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " +
                             "ORDER BY PF.CT DESC, PF.user_1 ASC, PF.user_2 ASC, P.PHOTO_ID ASC";
            ResultSet data = stmt.executeQuery(outdata);

            while (data.next()) {
                UserInfo u1 = new UserInfo(data.getLong(1), data.getString(2), data.getString(3));
                UserInfo u2 = new UserInfo(data.getLong(5), data.getString(6), data.getString(7));
                MatchPair mp = new MatchPair(u1, data.getLong(4), u2, data.getLong(8));
                PhotoInfo p = new PhotoInfo(data.getLong(9), data.getLong(11), data.getString(10), data.getString(12));
                mp.addSharedPhoto(p);

                for (int i = 0; i < data.getInt(13) - 1; i ++) {
                    data.next();
                    PhotoInfo p1 = new PhotoInfo(data.getLong(9), data.getLong(11), data.getString(10), data.getString(12));
                    mp.addSharedPhoto(p1);
                }
                results.add(mp);
            }


            String drop_1 = "DROP VIEW TAG_ID";
            String drop_2 = "DROP VIEW POT_FRI";
            stmt.executeQuery(drop_1);
            stmt.executeQuery(drop_2);

            data.close();
            stmt.close();

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
            String fr_all = "CREATE VIEW FR_ALL AS " +
                            "SELECT USER1_ID, USER2_ID " +
                            "FROM " + FriendsTable + " F " +
                            "UNION " +
                            "SELECT USER2_ID AS USER1_ID, USER1_ID AS USER2_ID " +
                            "FROM " + FriendsTable + " F";
            stmt.executeQuery(fr_all);

            String mu_all = "CREATE VIEW MU_ALL AS " +
                            "SELECT F1.USER1_ID AS ID1, F2.USER2_ID AS ID2, F1.USER2_ID AS MID " +
                            "FROM FR_ALL F1, FR_ALL F2 " +
                            "WHERE F1.USER2_ID = F2.USER1_ID AND F1.USER1_ID < F2.USER2_ID";
            stmt.executeQuery(mu_all);

            String selection = "CREATE VIEW SELECTION AS " +
                               "SELECT * " +
                               "FROM " +
                               "(SELECT M.ID1, M.ID2, COUNT(*) AS CT " +
                               "FROM MU_ALL M " +
                               "WHERE NOT EXISTS (SELECT * FROM " +
                                FriendsTable + " F " +
                               "WHERE M.ID1 = F.USER1_ID AND M.ID2 = F.USER2_ID) " +
                               "GROUP BY M.ID1, M.ID2 " +
                               "ORDER BY COUNT(*) DESC, M.ID1 ASC, M.ID2 ASC) " +
                               "WHERE ROWNUM <= " + num;
            stmt.executeQuery(selection);

            String info = "SELECT S.ID1, U1.FIRST_NAME, U1.LAST_NAME, S.ID2, U2.FIRST_NAME, U2.LAST_NAME, " +
                          "M.MID, U3.FIRST_NAME, U3.LAST_NAME, S.CT " +
                          "FROM SELECTION S, MU_ALL M, " + UsersTable + " U1, " + UsersTable + " U2, " + UsersTable + " U3 " +
                          "WHERE S.ID1 = M.ID1 AND S.ID2 = M.ID2 AND " +
                          "U1.USER_ID = S.ID1 AND U2.USER_ID = S.ID2 AND U3.USER_ID = M.MID " +
                          "ORDER BY S.CT DESC, S.ID1 ASC, S.ID2 ASC, M.MID ASC";
            ResultSet inf = stmt.executeQuery(info);

            while (inf.next()) {
                UserInfo u1 = new UserInfo(inf.getLong(1), inf.getString(2), inf.getString(3));
                UserInfo u2 = new UserInfo(inf.getLong(4), inf.getString(5), inf.getString(6));
                UsersPair up = new UsersPair(u1, u2);
                UserInfo u3 = new UserInfo(inf.getLong(7), inf.getString(8), inf.getString(9));
                up.addSharedFriend(u3);

                for (int i = 0; i < inf.getInt(10) - 1; i ++) {
                    inf.next();
                    UserInfo u3_n = new UserInfo(inf.getLong(7), inf.getString(8), inf.getString(9));
                    up.addSharedFriend(u3_n);
                }
                results.add(up);
            }

            String drop_1 = "DROP VIEW FR_ALL";
            String drop_2 = "DROP VIEW MU_ALL";
            String drop_3 = "DROP VIEW SELECTION";
            stmt.executeQuery(drop_1);
            stmt.executeQuery(drop_2);
            stmt.executeQuery(drop_3);

            inf.close();
            stmt.close();


        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */

            String state = "SELECT C.STATE_NAME, COUNT(E.EVENT_ID) " +
                           "FROM " + EventsTable + " E " +
                           "LEFT JOIN " + CitiesTable + " C " +
                           "ON E.EVENT_CITY_ID = C.CITY_ID " +
                           "GROUP BY C.STATE_NAME " +
                           "HAVING COUNT(E.EVENT_ID) = (SELECT MAX(COUNT(E1.EVENT_ID)) FROM " + EventsTable + " E1 " +
                           "LEFT JOIN " + CitiesTable + " C1 " +
                           "ON E1.EVENT_CITY_ID = C1.CITY_ID " +
                           "GROUP BY C1.STATE_NAME) " +
                           "ORDER BY C.STATE_NAME ASC";
            ResultSet st = stmt.executeQuery(state);
            st.next();
            EventStateInfo info = new EventStateInfo(st.getLong(2));
            info.addState(st.getString(1));
            while (st.next()) {
                info.addState(st.getString(1));
            }

            st.close();
            stmt.close();

            return info;                // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }
    
    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */

            String list = "CREATE VIEW Friend_List AS " +
                         "SELECT F.USER2_ID AS USER_ID " +
                         "FROM " + FriendsTable + " F " +
                         "WHERE F.USER1_ID = " + userID +
                         " UNION " +
                         "SELECT F.USER1_ID AS USER_ID " +
                         "FROM "+ FriendsTable + " F " +
                         "WHERE F.USER2_ID = " + userID +
                         " MINUS " +
                         "SELECT U.USER_ID " +
                         "FROM " + UsersTable + " U " +
                         "WHERE U.YEAR_OF_BIRTH IS NULL " +
                         "OR U.MONTH_OF_BIRTH IS NULL " +
                         "OR U.DAY_OF_BIRTH IS NULL";
            stmt.executeQuery(list);

            String young = "SELECT USER_ID, FIRST_NAME, LAST_NAME " +
                           "FROM " + UsersTable +
                           " WHERE USER_ID = " +
                           "(SELECT * FROM (SELECT FL.USER_ID " +
                           "FROM Friend_List FL, " + UsersTable + " U " +
                           "WHERE FL.USER_ID = U.USER_ID " +
                           "ORDER BY U.YEAR_OF_BIRTH DESC, U.MONTH_OF_BIRTH DESC, U.DAY_OF_BIRTH DESC, FL.USER_ID DESC) " +
                           "WHERE ROWNUM = 1)";
            ResultSet young_user = stmt.executeQuery(young);
            young_user.next();
            UserInfo youngsb = new UserInfo(young_user.getLong(1), young_user.getString(2), young_user.getString(3));
            young_user.close();

            String old = "SELECT USER_ID, FIRST_NAME, LAST_NAME " +
                         "FROM " + UsersTable +
                         " WHERE USER_ID = " +
                         "(SELECT * FROM (SELECT FL.USER_ID " +
                         "FROM Friend_List FL, " + UsersTable + " U " +
                         "WHERE FL.USER_ID = U.USER_ID " +
                         "ORDER BY U.YEAR_OF_BIRTH ASC, U.MONTH_OF_BIRTH ASC, U.DAY_OF_BIRTH ASC, FL.USER_ID DESC) " +
                         "WHERE ROWNUM = 1)";
            ResultSet old_user = stmt.executeQuery(old);
            old_user.next();
            UserInfo oldsb = new UserInfo(old_user.getLong(1), old_user.getString(2), old_user.getString(3));

            String dropview = "DROP VIEW Friend_List";
            stmt.executeQuery(dropview);

            old_user.close();
            stmt.close();

            return new AgeInfo(oldsb, youngsb);                // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }
    
    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */

            String sibling = "SELECT F.USER1_ID, F.USER2_ID, U1.First_Name, U1.Last_Name, U2.First_Name, U2.Last_Name " +
                             "FROM " + UsersTable + " U1, " + UsersTable + " U2, " + FriendsTable  + " F, " + 
                             HometownCitiesTable + " H1, " + HometownCitiesTable + " H2 " +
                             "WHERE U1.YEAR_OF_BIRTH IS NOT NULL AND U2.YEAR_OF_BIRTH IS NOT NULL AND " +
                             "U1.USER_ID = H1.USER_ID AND U2.USER_ID = H2.USER_ID " +
                             "AND H1.HOMETOWN_CITY_ID = H2.HOMETOWN_CITY_ID " +
                             "AND U1.USER_ID = F.USER1_ID AND U2.USER_ID = F.USER2_ID " +
                             "AND U1.LAST_NAME = U2.LAST_NAME " +
                             "AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) < 10 " +
                             "ORDER BY F.USER1_ID ASC, F.USER2_ID ASC";
            ResultSet sib = stmt.executeQuery(sibling);
            while (sib.next()) {
                UserInfo u1 = new UserInfo(sib.getLong(1), sib.getString(3), sib.getString(4));
                UserInfo u2 = new UserInfo(sib.getLong(2), sib.getString(5), sib.getString(6));
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            }

            sib.close();
            stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
