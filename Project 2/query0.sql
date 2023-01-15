SELECT COUNT(*) AS Birthed, Month_of_Birth
FROM jiaqni.PUBLIC_Users
WHERE Month_of_Birth IS NOT NULL
GROUP BY Month_of_Birth
ORDER BY Birthed DESC, Month_of_Birth ASC;

SELECT User_ID, First_Name, Last_Name
FROM jiaqni.PUBLIC_Users
WHERE Month_of_Birth = 9
ORDER BY User_ID;


SELECT User_ID, First_Name, Last_Name
FROM jiaqni.PUBLIC_Users
WHERE Month_of_Birth = 6
ORDER BY User_ID;



try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
             Statement stmt2 = ....) 