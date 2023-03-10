CREATE VIEW Lonely AS
(SELECT U1.USER_ID
FROM jiaqni.PUBLIC_Users U1
MINUS 
SELECT F1.USER1_ID
FROM jiaqni.PUBLIC_Friends F1)
INTERSECT 
(SELECT U2.USER_ID
FROM jiaqni.PUBLIC_Users U2
MINUS 
SELECT F2.USER2_ID
FROM jiaqni.PUBLIC_Friends F2);


SELECT U.User_ID, U.First_Name, U.Last_Name
FROM jiaqni.PUBLIC_Users U, Lonely L
WHERE U.USER_ID = L.USER_ID
ORDER BY U.USER_ID ASC;


DROP VIEW Lonely;

