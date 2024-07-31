-- Adding bird_dev_financial examples into the Example Store
-- Source: google3/cloud/databases/nl2sql/datasets/birdsql/bird_dev.json

SELECT alloydb_ai_nl.g_admit_example('How many accounts who choose issuance after transaction are staying in East Bohemia region?', 'SELECT COUNT(T1.district_id) FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id WHERE T1."A3" = ''East Bohemia'' AND T2.frequency = ''POPLATEK PO OBRATU''', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many accounts who have region in Prague are eligible for loans?', 'SELECT COUNT(T1.account_id) FROM public.account AS T1 INNER JOIN public.loan AS T2 ON T1.account_id = T2.account_id INNER JOIN public.district AS T3 ON T1.district_id = T3.district_id WHERE T3."A3" = ''Prague''', 'public');

-- The SQL does not parse:
-- SELECT alloydb_ai_nl.g_admit_example('The average unemployment ratio of 1995 and 1996, which one has higher percentage?', 'SELECT DISTINCT IIF(AVG("A13") > AVG("A12"), CAST(''1996'' as text), CAST(''1995'' as text)) FROM public.district', 'public');


SELECT alloydb_ai_nl.g_admit_example('List out the no. of districts that have female average salary is more than 6000 but less than 10000?', 'SELECT DISTINCT T2.district_id FROM public.client AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T1.gender = ''F'' AND T2."A11" BETWEEN 6000 AND 10000', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many male customers who are living in North Bohemia have average salary greater than 8000?', 'SELECT COUNT(T1.client_id) FROM public.client AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T1.gender = ''M'' AND T2."A3" = ''North Bohemia'' AND T2."A11" > 8000', 'public');


SELECT alloydb_ai_nl.g_admit_example('List out the account numbers of female clients who are oldest and has lowest average salary, calculate the gap between this lowest average salary with the highest average salary?', 'SELECT T1.account_id , ( SELECT MAX("A11") - MIN("A11") FROM public.district ) FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T2.district_id = ( SELECT district_id FROM public.client WHERE gender = ''F'' ORDER BY birth_date ASC LIMIT 1 ) ORDER BY T2."A11" DESC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('List out the account numbers of clients who are youngest and have highest average salary?', 'SELECT T1.account_id FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T2.district_id = ( SELECT district_id FROM public.client ORDER BY birth_date DESC LIMIT 1 ) ORDER BY T2."A11" DESC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many customers who choose statement of weekly issuance are Owner?', 'SELECT COUNT(T1.account_id) FROM public.account AS T1 INNER JOIN public.disp AS T2 ON T1.account_id = T2.account_id WHERE T2.type = ''Owner'' AND T1.frequency = ''POPLATEK TYDNE''', 'public');


SELECT alloydb_ai_nl.g_admit_example('List out the clients who choose statement of issuance after transaction are Disponent?', 'SELECT T2.client_id FROM public.account AS T1 INNER JOIN public.disp AS T2 ON T1.account_id = T2.account_id WHERE T1.frequency = ''POPLATEK PO OBRATU'' AND T2.type = ''DISPONENT''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Among the accounts who have approved loan date in 1997, list out the accounts that have the lowest approved amount and choose weekly issuance statement.', 'SELECT T2.account_id FROM public.loan AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id WHERE to_char(T1.date, ''YYYY'') = ''1997'' AND T2.frequency = ''POPLATEK TYDNE'' ORDER BY T1.amount LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('Among the accounts who have loan validity more than 12 months, list out the accounts that have the highest approved amount and have account opening date in 1993.', 'SELECT T1.account_id FROM public.loan AS T1 INNER JOIN public.disp AS T2 ON T1.account_id = T2.account_id WHERE to_char(T1.date, ''YYYY'') = ''1993'' AND T1.duration = 12 ORDER BY T1.amount DESC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('Among the account opened, how many female customers who were born before 1950 and stayed in Slokolov?', 'SELECT COUNT(T2.client_id) FROM public.district AS T1 INNER JOIN public.client AS T2 ON T1.district_id = T2.district_id WHERE T2.gender = ''F'' AND to_char(T2.birth_date, ''YYYY'') < ''1950'' AND T1."A2" = ''Slokolov''', 'public');


SELECT alloydb_ai_nl.g_admit_example('List out the accounts who have the earliest trading date in 1995 ?', 'SELECT account_id FROM public.trans WHERE to_char(date, ''YYYY'') = ''1995'' ORDER BY date ASC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('State different accounts who have account opening date before 1997 and own an amount of money greater than 3000USD', 'SELECT DISTINCT T2.account_id FROM public.trans AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id WHERE to_char(T2.date, ''YYYY'') < ''1997'' AND T1.amount > 3000', 'public');


SELECT alloydb_ai_nl.g_admit_example('Which client issued his/her card in 1994/3/3, give his/her client id.', 'SELECT T2.client_id FROM public.client AS T1 INNER JOIN public.disp AS T2 ON T1.client_id = T2.client_id INNER JOIN public.card AS T3 ON T2.disp_id = T3.disp_id WHERE T3.issued = ''1994-03-03''', 'public');


SELECT alloydb_ai_nl.g_admit_example('The transaction of 840 USD happened in 1998/10/14, when was this account opened?', 'SELECT T1.date FROM public.account AS T1 INNER JOIN public.trans AS T2 ON T1.account_id = T2.account_id WHERE T2.amount = 840 AND T2.date = ''1998-10-14''', 'public');


SELECT alloydb_ai_nl.g_admit_example('There was a loan approved in 1994/8/25, where was that account opened, give the district Id of the branch.', 'SELECT T1.district_id FROM public.account AS T1 INNER JOIN public.loan AS T2 ON T1.account_id = T2.account_id WHERE T2.date = ''1994-08-25''', 'public');


SELECT alloydb_ai_nl.g_admit_example('What is the biggest amount of transaction that the client whose card was opened in 1996/10/21 made?', 'SELECT T2.amount FROM public.account AS T1 INNER JOIN public.trans AS T2 ON T1.account_id = T2.account_id WHERE T1.date = ''1996-10-21'' ORDER BY T2.amount DESC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('What is the gender of the oldest client who opened his/her account in the highest average salary branch?', 'SELECT T2.gender FROM public.district AS T1 INNER JOIN public.client AS T2 ON T1.district_id = T2.district_id ORDER BY T1."A11" DESC, T2.birth_date ASC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('For the client who applied the biggest loan, what was his/her first amount of transaction after opened the account?', 'SELECT T2.amount FROM public.loan AS T1 INNER JOIN public.trans AS T2 ON T1.account_id = T2.account_id ORDER BY T1.amount DESC, T2.date ASC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many clients opened their accounts in Jesenik branch were women?', 'SELECT COUNT(T1.client_id) FROM public.client AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T1.gender = ''F'' AND T2."A2" = ''Jesenik''', 'public');


SELECT alloydb_ai_nl.g_admit_example('What is the disposition id of the client who made 5100 USD transaction in 1998/9/2?', 'SELECT T1.disp_id FROM public.disp AS T1 INNER JOIN public.trans AS T2 ON T1.account_id = T2.account_id WHERE T2.date = ''1998-09-02'' AND T2.amount = 5100', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many accounts were opened in Litomerice in 1996?', 'SELECT COUNT(T2.account_id) FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id WHERE to_char(T2.date, ''YYYY'') = ''1996'' AND T1."A2" = ''Litomerice''', 'public');


SELECT alloydb_ai_nl.g_admit_example('For the female client who was born in 1976/1/29, which district did she opened her account?', 'SELECT T1."A2" FROM public.district AS T1 INNER JOIN public.client AS T2 ON T1.district_id = T2.district_id WHERE T2.birth_date = ''1976-01-29'' AND T2.gender = ''F''', 'public');


SELECT alloydb_ai_nl.g_admit_example('For the client who applied 98832 USD loan in 1996/1/3, when was his/her birthday?', 'SELECT T3.birth_date FROM public.loan AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id INNER JOIN public.client AS T3 ON T2.district_id = T3.district_id WHERE T1.date = ''1996-01-03'' AND T1.amount = 98832', 'public');


SELECT alloydb_ai_nl.g_admit_example('For the first client who opened his/her account in Prague, what is his/her account ID?', 'SELECT T1.account_id FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T2."A3" = ''Prague'' ORDER BY T1.date ASC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('For the branch which located in the south Bohemia with biggest number of inhabitants, what is the percentage of the male clients?', 'SELECT CAST(SUM(CAST(T1.gender = ''M'' as integer)) AS REAL) * 100 / COUNT(T1.client_id) FROM public.client AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T2."A3" = ''south Bohemia'' GROUP BY T2."A4" ORDER BY T2."A4" DESC LIMIT 1', 'public');

-- The SQL does not parse:
-- SELECT alloydb_ai_nl.g_admit_example('For the client who first applied the loan in 1993/7/5, what is the increase rate of his/her account balance from 1993/3/22 to 1998/12/27?', 'SELECT CAST((SUM(IIF(T3.date = ''1998-12-27'', T3.balance::real, 0::real)) - SUM(IIF(T3.date = ''1993-03-22'', T3.balance::real, 0::real))) AS REAL) * 100 / SUM(IIF(T3.date = ''1993-03-22'', T3.balance::real, 0::real)) FROM public.loan AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id INNER JOIN public.trans AS T3 ON T3.account_id = T2.account_id WHERE T1.date = ''1993-07-05''', 'public');

-- The SQL does not parse:
-- SELECT alloydb_ai_nl.g_admit_example('What is the percentage of loan amount that has been fully paid with no issue.', 'SELECT CAST(SUM(cast(status = ''A'' as integer)) AS REAL) * 100 / COUNT(amount) FROM loan', 'public');


SELECT alloydb_ai_nl.g_admit_example('For loan amount less than USD100,000, what is the percentage of accounts that is still running with no issue.', 'SELECT CAST(SUM(CAST(status = ''C'' as integer)) AS REAL) * 100 / COUNT(amount) FROM public.loan WHERE amount < 100000', 'public');


SELECT alloydb_ai_nl.g_admit_example('For accounts in 1993 with statement issued after transaction, list the account ID, district name and district region.', 'SELECT T1.account_id, T2."A2", T2."A3" FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T1.frequency = ''POPLATEK PO OBRATU'' AND to_char(T1.date, ''YYYY'')= ''1993''', 'public');


SELECT alloydb_ai_nl.g_admit_example('From Year 1995 to 2000, who are the accounts holders from ''east Bohemia''. State the account ID the frequency of statement issuance.', 'SELECT T1.account_id, T1.frequency FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T2."A3" = ''east Bohemia'' AND to_char(T1.date, ''YYYY'') BETWEEN ''1995'' AND ''2000''', 'public');


SELECT alloydb_ai_nl.g_admit_example('List account ID and account opening date for accounts from ''Prachatice''.', 'SELECT T1.account_id, T1.date FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T2."A2" = ''Prachatice''', 'public');


SELECT alloydb_ai_nl.g_admit_example('State the district and region for loan ID ''4990''.', 'SELECT T2."A2", T2."A3" FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id INNER JOIN public.loan AS T3 ON T1.account_id = T3.account_id WHERE T3.loan_id = 4990', 'public');


SELECT alloydb_ai_nl.g_admit_example('Provide the account ID, district and region for loan amount greater than USD300,000.', 'SELECT T1.account_id, T2."A2", T2."A3" FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id INNER JOIN public.loan AS T3 ON T1.account_id = T3.account_id WHERE T3.amount > 300000', 'public');


SELECT alloydb_ai_nl.g_admit_example('List the loan ID, district and average salary for loan with duration of 60 months.', 'SELECT T3.loan_id, T2."A2", T2."A11" FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id INNER JOIN public.loan AS T3 ON T1.account_id = T3.account_id WHERE T3.duration = 60', 'public');


SELECT alloydb_ai_nl.g_admit_example('For loans contracts which are still running where client are in debt, list the district of the and the state the percentage unemployment rate increment from year 1995 to 1996.', 'SELECT CAST((T3."A13" - T3."A12") AS REAL) * 100 / T3."A12" FROM public.loan AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id INNER JOIN public.district AS T3 ON T2.district_id = T3.district_id WHERE T1.status = ''D''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Calculate the percentage of account from ''Decin'' district for all accounts are opened in 1993.', 'SELECT CAST(SUM(cast(T1."A2" = ''Decin'' as integer)) AS REAL) * 100 / COUNT(account_id) FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id WHERE to_char(T2.date, ''YYYY'') = ''1993''', 'public');


SELECT alloydb_ai_nl.g_admit_example('List the account IDs with monthly issuance of statements.', 'SELECT account_id FROM public.account WHERE Frequency = ''POPLATEK MESICNE''', 'public');


SELECT alloydb_ai_nl.g_admit_example('List the top ten districts, by descending order, from the highest to the lowest, the number of female account holders.', 'SELECT T2."A2", COUNT(T1.client_id) FROM public.client AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T1.gender = ''F'' GROUP BY T2.district_id, T2."A2" ORDER BY COUNT(T1.client_id) DESC LIMIT 10', 'public');


SELECT alloydb_ai_nl.g_admit_example('Which are the top ten withdrawals (non-credit card) by district names for the month of January 1996?', 'SELECT T1.district_id FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id INNER JOIN public.trans AS T3 ON T2.account_id = T3.account_id WHERE T3.type = ''VYDAJ'' AND CAST(T2.date as text) LIKE ''1996-01%'' ORDER BY "A2" ASC LIMIT 10', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many of the account holders in South Bohemia still do not own credit cards?', 'SELECT COUNT(T3.account_id) FROM public.district AS T1 INNER JOIN public.client AS T2 ON T1.district_id = T2.district_id INNER JOIN public.disp AS T3 ON T2.client_id = T3.client_id WHERE T1."A3" = ''south Bohemia'' AND T3.type != ''OWNER''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Which district has highest active loan?', 'SELECT T2."A3" FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id INNER JOIN public.loan AS T3 ON T1.account_id = T3.account_id WHERE T3.status IN (''C'', ''D'') GROUP BY T2."A3" ORDER BY SUM(T3.amount) DESC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('What is the average loan amount by male borrowers?', 'SELECT AVG(T3.amount) FROM public.client AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id INNER JOIN public.loan AS T3 ON T2.account_id = T3.account_id WHERE T1.gender = ''M''', 'public');


SELECT alloydb_ai_nl.g_admit_example('In 1996, which districts have the highest unemployment rate? List their branch location and district name.', 'SELECT district_id, "A2" FROM public.district ORDER BY "A13" DESC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('In the branch where the largest number of crimes were committed in 1996, how many accounts were opened?', 'SELECT COUNT(T2.account_id) FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id GROUP BY T1."A16" ORDER BY T1."A16" DESC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('After making a credit card withdrawal, how many account/s with monthly issuance has a negative balance?', 'SELECT COUNT(T1.account_id) FROM public.trans AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id WHERE T1.balance < 0 AND T1.operation = ''VYBER KARTOU'' AND T2.frequency = ''POPLATEK MESICNE''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Between 1/1/1995 and 12/31/1997, how many loans in the amount of at least 250,000 per account that chose monthly statement issuance were approved?', 'SELECT COUNT(T1.account_id) FROM public.account AS T1 INNER JOIN public.loan AS T2 ON T1.account_id = T2.account_id WHERE T2.date BETWEEN ''1995-01-01'' AND ''1997-12-31'' AND T1.frequency = ''POPLATEK MESICNE'' AND T2.amount > 250000', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many accounts have running contracts in Branch location 1?', 'SELECT COUNT(T1.account_id) FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id INNER JOIN public.loan AS T3 ON T1.account_id = T3.account_id WHERE T1.district_id = 1 AND (T3.status = ''C'' OR T3.status = ''D'')', 'public');


SELECT alloydb_ai_nl.g_admit_example('In the branch where the second-highest number of crimes were committed in 1995 occurred, how many male clients are there?', 'SELECT COUNT(T1.client_id) FROM public.client AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T1.gender = ''M'' AND T2."A15" = (SELECT T3."A15" FROM public.district AS T3 ORDER BY T3."A15" DESC LIMIT 1 OFFSET 1)', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many high-level credit cards have "disponent" type of disposition?', 'SELECT COUNT(T1.card_id) FROM public.card AS T1 INNER JOIN public.disp AS T2 ON T1.disp_id = T2.disp_id WHERE T1.type = ''gold'' AND T2.type = ''DISPONENT''', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many accounts are there in the district of "Pisek"?', 'SELECT COUNT(T1.account_id) FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T2."A2" = ''Pisek''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Which districts have transactions greater than USS$10,000 in 1997?', 'SELECT T1.district_id FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id INNER JOIN public.trans AS T3 ON T1.account_id = T3.account_id WHERE to_char(T3.date, ''YYYY'') = ''1997'' GROUP BY T1.district_id HAVING SUM(T3.amount) > 10000', 'public');


SELECT alloydb_ai_nl.g_admit_example('Which accounts placed orders for household payment in Pisek?', 'SELECT DISTINCT T2.account_id FROM public.trans AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id INNER JOIN public.district AS T3 ON T2.district_id = T3.district_id WHERE T1.k_symbol = ''SIPO'' AND T3."A2" = ''Pisek''', 'public');


SELECT alloydb_ai_nl.g_admit_example('What are the accounts that have both gold and junior credit cards?', 'SELECT T2.account_id FROM public.card AS T1 INNER JOIN public.disp AS T2 ON T1.disp_id = T2.disp_id WHERE T1.type IN (''gold'', ''junior'')', 'public');


SELECT alloydb_ai_nl.g_admit_example('How much is the average amount in credit card made by account holders in a month, in year 2021?', 'SELECT AVG(T3.amount) FROM public.card AS T1 INNER JOIN public.disp AS T2 ON T1.disp_id = T2.disp_id INNER JOIN public.trans AS T3 ON T2.account_id = T3.account_id WHERE to_char(T3.date, ''YYYY'') = ''2021'' AND T3.operation = ''VYBER KARTOU''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Who are the account holder identification numbers whose spent per month on the credit card is less than the average, in 1998?', 'SELECT T1.account_id FROM public.trans AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id WHERE to_char(T1.date, ''YYYY'') = ''1998'' AND T1.operation = ''VYBER KARTOU'' AND T1.amount < (SELECT AVG(amount) FROM public.trans WHERE to_char(date, ''YYYY'') = ''1998'')', 'public');


SELECT alloydb_ai_nl.g_admit_example('Who are the female account holders who own credit cards and also have loans?', 'SELECT T1.client_id FROM public.client AS T1 INNER JOIN public.disp AS T2 ON T1.client_id = T2.client_id INNER JOIN public.loan AS T3 ON T2.account_id = T3.account_id INNER JOIN public.card AS T4 ON T2.disp_id = T4.disp_id WHERE T1.gender = ''F''', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many female clients'' accounts are in the region of South Bohemia?', 'SELECT COUNT(T1.client_id) FROM public.client AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T1.gender = ''F'' AND T2."A3" = ''south Bohemia''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Please list the accounts whose district is Tabor that are eligible for loans.', 'SELECT T2.account_id FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id INNER JOIN public.disp AS T3 ON T2.account_id = T3.account_id WHERE T3.type = ''OWNER'' AND T1."A2" = ''Tabor''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Please list the account types that are not eligible for loans, and the average income of residents in the district where the account is located exceeds $8000 but is no more than $9000.', 'SELECT T3.type FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id INNER JOIN public.disp AS T3 ON T2.account_id = T3.account_id WHERE T3.type != ''OWNER'' AND T1."A11" BETWEEN 8000 AND 9000', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many accounts in North Bohemia has made a transaction with the partner''s bank being AB?', 'SELECT COUNT(T2.account_id) FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id INNER JOIN public.trans AS T3 ON T2.account_id = T3.account_id WHERE T3.bank = ''AB'' AND T1."A3" = ''north Bohemia''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Please list the name of the districts with accounts that made withdrawal transactions.', 'SELECT DISTINCT T1."A2" FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id INNER JOIN public.trans AS T3 ON T2.account_id = T3.account_id WHERE T3.type = ''VYDAJ''', 'public');


SELECT alloydb_ai_nl.g_admit_example('What is the average number of crimes committed in 1995 in regions where the number exceeds 4000 and the region has accounts that are opened starting from the year 1997?', 'SELECT AVG(T1."A15") FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id WHERE to_char(T2.date, ''YYYY'') >= ''1997'' AND T1."A15" > 4000', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many ''classic'' cards are eligible for loan?', 'SELECT COUNT(T1.card_id) FROM public.card AS T1 INNER JOIN public.disp AS T2 ON T1.disp_id = T2.disp_id WHERE T1.type = ''classic'' AND T2.type = ''Owner''', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many male clients in ''Hl.m. Praha'' district?', 'SELECT COUNT(T1.client_id) FROM public.client AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE T1.gender = ''M'' AND T2."A2" = ''Hl.m. Praha''', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many percent of ''Gold'' cards were issued prior to 1998?', 'SELECT CAST(SUM(cast(type = ''gold'' as integer)) AS REAL) * 100 / COUNT(card_id) FROM public.card WHERE to_char(issued, ''YYYY'') < ''1998''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Who is the owner of the account with the largest loan amount?', 'SELECT T1.client_id FROM public.disp AS T1 INNER JOIN public.loan AS T2 ON T1.account_id = T2.account_id WHERE T1.type = ''OWNER'' ORDER BY T2.amount DESC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('What is the number of committed crimes in 1995 in the district of the account with the id 532?', 'SELECT T1."A15" FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id WHERE T2.account_id = 532', 'public');


SELECT alloydb_ai_nl.g_admit_example('What is the district Id of the account that placed the order with the id 33333?', 'SELECT T3.district_id FROM public.order AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id INNER JOIN public.district AS T3 ON T2.district_id = T3.district_id WHERE T1.order_id = 33333', 'public');


SELECT alloydb_ai_nl.g_admit_example('List all the withdrawals in cash transactions that the client with the id 3356 makes.', 'SELECT T4.trans_id FROM public.client AS T1 INNER JOIN public.disp AS T2 ON T1.client_id = T2.client_id INNER JOIN public.account AS T3 ON T2.account_id = T3.account_id INNER JOIN public.trans AS T4 ON T3.account_id = T4.account_id WHERE T1.client_id = 3356 AND T4.operation = ''VYBER''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Among the weekly issuance accounts, how many have a loan of under 200000?', 'SELECT COUNT(T1.account_id) FROM public.loan AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id WHERE T2.frequency = ''POPLATEK TYDNE'' AND T1.amount < 200000', 'public');


SELECT alloydb_ai_nl.g_admit_example('What type of credit card does the client with the id 13539 own?', 'SELECT T3.type FROM public.disp AS T1 INNER JOIN public.client AS T2 ON T1.client_id = T2.client_id INNER JOIN public.card AS T3 ON T1.disp_id = T3.disp_id WHERE T2.client_id = 13539', 'public');


SELECT alloydb_ai_nl.g_admit_example('What is the region of the client with the id 3541 from?', 'SELECT T2.district_id, T1."A3" FROM public.district AS T1 INNER JOIN public.client AS T2 ON T1.district_id = T2.district_id WHERE T2.client_id = 3541', 'public');


SELECT alloydb_ai_nl.g_admit_example('Which district has the most accounts with loan contracts finished with no problems?', 'SELECT T1.district_id FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id INNER JOIN public.loan AS T3 ON T2.account_id = T3.account_id WHERE T3.status = ''A'' GROUP BY T1.district_id ORDER BY COUNT(T2.account_id) DESC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('Who placed the order with the id 32423?', 'SELECT T3.client_id FROM public.order AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id INNER JOIN public.client AS T3 ON T2.district_id = T3.district_id WHERE T1.order_id = 32423', 'public');


SELECT alloydb_ai_nl.g_admit_example('Please list all the transactions made by accounts from district 5.', 'SELECT T3.trans_id FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id INNER JOIN public.trans AS T3 ON T2.account_id = T3.account_id WHERE T1.district_id = 5', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many of the accounts are from Jesenik district?', 'SELECT COUNT(T2.account_id) FROM public.district AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id WHERE T1."A2" = ''Jesenik''', 'public');


SELECT alloydb_ai_nl.g_admit_example('List all the clients'' IDs whose junior credit cards were issued after 1996.', 'SELECT T2.client_id FROM public.card AS T1 INNER JOIN public.disp AS T2 ON T1.disp_id = T2.disp_id WHERE T1.type = ''junior'' AND T1.issued >= ''1997-01-01''', 'public');


SELECT alloydb_ai_nl.g_admit_example('What percentage of clients who opened their accounts in the district with an average salary of over 10000 are women?', 'SELECT CAST(SUM(cast(T2.gender = ''F'' as integer)) AS REAL) * 100 / COUNT(T2.client_id) FROM public.district AS T1 INNER JOIN public.client AS T2 ON T1.district_id = T2.district_id WHERE T1."A11" > 10000', 'public');


SELECT alloydb_ai_nl.g_admit_example('What was the growth rate of the total amount of loans across all accounts for a male client between 1996 and 1997?', 'SELECT CAST((SUM(CASE WHEN to_char(T1.date, ''YYYY'') = ''1997'' THEN T1.amount ELSE 0 END) - SUM(CASE WHEN to_char(T1.date, ''YYYY'') = ''1996'' THEN T1.amount ELSE 0 END)) AS REAL) * 100 / SUM(CASE WHEN to_char(T1.date, ''YYYY'') = ''1996'' THEN T1.amount ELSE 0 END) FROM public.loan AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id INNER JOIN public.disp AS T3 ON T3.account_id = T2.account_id INNER JOIN public.client AS T4 ON T4.client_id = T3.client_id WHERE T4.gender = ''M'' AND T3.type = ''OWNER''', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many credit card withdrawals were recorded after 1995?', 'SELECT COUNT(account_id) FROM public.trans WHERE to_char(date, ''YYYY'') > ''1995'' AND operation = ''VYBER KARTOU''', 'public');


-- The SQL does not parse:
-- SELECT alloydb_ai_nl.g_admit_example('What was the difference in the number of crimes committed in East and North Bohemia in 1996?', 'SELECT SUM(IIF("A3" = ''East Bohemia'', CAST("A16" as integer), 0)) - SUM(IIF("A3" = ''North Bohemia'', CAST("A16" as integer), 0)) FROM public.district', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many owner and disponent dispositions are there from account number 1 to account number 10?', 'SELECT SUM(cast(type = ''OWNER'' as integer)) , SUM(cast(type = ''DISPONENT'' as integer)) FROM public.disp WHERE account_id BETWEEN 1 AND 10', 'public');


SELECT alloydb_ai_nl.g_admit_example('How often does account number 3 request an account statement to be released? What was the aim of debiting 3539 in total?', 'SELECT T1.frequency, T2.k_symbol FROM public.account AS T1 INNER JOIN public.order AS T2 ON T1.account_id = T2.account_id WHERE T1.account_id = 3 AND T2.amount = 3539', 'public');


SELECT alloydb_ai_nl.g_admit_example('What year was account owner number 130 born?', 'SELECT to_char(T1.birth_date, ''YYYY'') FROM public.client AS T1 INNER JOIN public.disp AS T3 ON T1.client_id = T3.client_id INNER JOIN public.account AS T2 ON T3.account_id = T2.account_id WHERE T2.account_id = 130', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many accounts have an owner disposition and request for a statement to be generated upon a transaction?', 'SELECT COUNT(T1.account_id) FROM public.account AS T1 INNER JOIN public.disp AS T2 ON T1.account_id = T2.account_id WHERE T2.type = ''OWNER'' AND T1.frequency = ''POPLATEK PO OBRATU''', 'public');


SELECT alloydb_ai_nl.g_admit_example('What is the amount of debt that client number 992 has, and how is this client doing with payments?', 'SELECT T3.amount, T3.status FROM public.client AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id INNER JOIN public.loan AS T3 ON T2.account_id = T3.account_id WHERE T1.client_id = 992', 'public');


SELECT alloydb_ai_nl.g_admit_example('What is the sum that client number 4''s account has following transaction 851? Who owns this account, a man or a woman?', 'SELECT T3.balance, T1.gender FROM public.client AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id INNER JOIN public.trans AS T3 ON T2.account_id = T3.account_id WHERE T1.client_id = 4 AND T3.trans_id = 851', 'public');


SELECT alloydb_ai_nl.g_admit_example('Which kind of credit card does client number 9 possess?', 'SELECT T3.type FROM public.client AS T1 INNER JOIN public.disp AS T2 ON T1.client_id = T2.client_id INNER JOIN public.card AS T3 ON T2.disp_id = T3.disp_id WHERE T1.client_id = 9', 'public');


SELECT alloydb_ai_nl.g_admit_example('How much, in total, did client number 617 pay for all of the transactions in 1998?', 'SELECT SUM(T3.amount) FROM public.client AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id INNER JOIN public.trans AS T3 ON T2.account_id = T3.account_id WHERE to_char(T3.date, ''YYYY'')= ''1998'' AND T1.client_id = 617', 'public');


SELECT alloydb_ai_nl.g_admit_example('Please provide a list of clients who were born between 1983 and 1987 and whose account branch is in East Bohemia, along with their IDs.', 'SELECT T1.client_id, T3.account_id FROM public.client AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id INNER JOIN public.account AS T3 ON T2.district_id = T3.district_id WHERE T2."A3" = ''east Bohemia'' AND to_char(T1.birth_date, ''YYYY'') BETWEEN ''1983'' AND ''1987''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Please provide the IDs of the 3 female clients with the largest loans.', 'SELECT T1.client_id FROM public.client AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id INNER JOIN public.loan AS T3 ON T2.account_id = T3.account_id WHERE T1.gender = ''F'' ORDER BY T3.amount DESC LIMIT 3', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many male customers who were born between 1974 and 1976 have made a payment on their home in excess of $4000?', 'SELECT COUNT(T1.account_id) FROM public.trans AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id INNER JOIN public.client AS T3 ON T2.district_id = T3.district_id WHERE to_char(T3.birth_date, ''YYYY'') BETWEEN ''1974'' AND ''1976'' AND T3.gender = ''M'' AND T1.amount > 4000 AND T1.k_symbol = ''SIPO''', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many accounts in Beroun were opened after 1996?', 'SELECT COUNT(account_id) FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE to_char(T1.date, ''YYYY'') > ''1996'' AND T2."A2" = ''Beroun''', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many female customers have a junior credit card?', 'SELECT COUNT(T1.client_id) FROM public.client AS T1 INNER JOIN public.disp AS T2 ON T1.client_id = T2.client_id INNER JOIN public.card AS T3 ON T2.disp_id = T3.disp_id WHERE T1.gender = ''F'' AND T3.type = ''junior''', 'public');


SELECT alloydb_ai_nl.g_admit_example('What proportion of customers who have accounts at the Prague branch are female?', 'SELECT CAST(SUM(cast(T2.gender = ''F'' as integer)) AS REAL) / COUNT(T2.client_id) * 100 FROM public.district AS T1 INNER JOIN public.client AS T2 ON T1.district_id = T2.district_id WHERE T1."A3" = ''Prague''', 'public');


SELECT alloydb_ai_nl.g_admit_example('What percentage of male clients request for weekly statements to be issued?', 'SELECT CAST(SUM(cast(T1.gender = ''M'' as integer)) AS REAL) * 100 / COUNT(T1.client_id) FROM public.client AS T1 INNER JOIN public.account AS T2 ON T1.district_id = T2.district_id WHERE T2.frequency = ''POPLATEK TYDNE''', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many clients who choose statement of weekly issuance are User?', 'SELECT COUNT(T2.account_id) FROM public.account AS T1 INNER JOIN public.disp AS T2 ON T2.account_id = T1.account_id WHERE T1.frequency = ''POPLATEK TYDNE'' AND T2.type = ''USER''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Among the accounts who have loan validity more than 24 months, list out the accounts that have the lowest approved amount and have account opening date before 1997.', 'SELECT T1.account_id FROM public.loan AS T1 INNER JOIN public.account AS T2 ON T1.account_id = T2.account_id WHERE T1.duration > 24 AND to_char(T2.date, ''YYYY'') < ''1997'' ORDER BY T1.amount ASC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('Name the account numbers of female clients who are oldest and have lowest average salary?', 'SELECT T3.account_id FROM public.client AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id INNER JOIN public.account AS T3 ON T2.district_id = T3.district_id WHERE T1.gender = ''F'' ORDER BY T1.birth_date ASC, T2."A11" ASC LIMIT 1', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many clients who were born in 1920 stay in east Bohemia?', 'SELECT COUNT(T1.client_id) FROM public.client AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id WHERE to_char(T1.birth_date, ''YYYY'') = ''1920'' AND T2."A3" = ''east Bohemia''', 'public');


SELECT alloydb_ai_nl.g_admit_example('How many loan accounts are for pre-payment of duration of 24 months with weekly issuance of statement.', 'SELECT COUNT(T2.account_id) FROM public.account AS T1 INNER JOIN public.loan AS T2 ON T1.account_id = T2.account_id WHERE T2.duration = 24 AND T1.frequency = ''POPLATEK TYDNE''', 'public');


SELECT alloydb_ai_nl.g_admit_example('What is the average amount of loan which are still on running contract with statement issuance after each transaction?', 'SELECT AVG(T2.payments) FROM public.account AS T1 INNER JOIN public.loan AS T2 ON T1.account_id = T2.account_id WHERE T2.status IN (''C'', ''D'') AND T1.frequency = ''POPLATEK PO OBRATU''', 'public');


SELECT alloydb_ai_nl.g_admit_example('List all ID and district for clients that can only have the right to issue permanent orders or apply for loans.', 'SELECT T3.client_id, T2.district_id, T2."A2" FROM public.account AS T1 INNER JOIN public.district AS T2 ON T1.district_id = T2.district_id INNER JOIN public.disp AS T3 ON T1.account_id = T3.account_id WHERE T3.type = ''OWNER''', 'public');


SELECT alloydb_ai_nl.g_admit_example('Provide the IDs and age of the client with high level credit card, which is eligible for loans.', 'SELECT T1.client_id, CAST(to_char(CURRENT_TIMESTAMP, ''YYYY'') as integer) - CAST(to_char(T3.birth_date, ''YYYY'') as integer) FROM public.disp AS T1 INNER JOIN public.card AS T2 ON T2.disp_id = T1.disp_id INNER JOIN public.client AS T3 ON T1.client_id = T3.client_id WHERE T2.type = ''gold'' AND T1.type = ''OWNER''', 'public');
