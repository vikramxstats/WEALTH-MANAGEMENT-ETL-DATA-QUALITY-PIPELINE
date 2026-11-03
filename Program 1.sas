/******************************************************************************
  PROJECT: Wealth Management ETL & Data Quality Pipeline
  AUTHOR: Vikram Singh
  PURPOSE: Multi-source ETL join via PROC SQL, Rule-Based Data Quality 
           Quarantine Engine, Reconciliation Control-Totals, and 90-Day Churn Risk.
******************************************************************************/

%let filepath = /home/u64626905/wealth_management/;

/* ---------------------------------------------------------------------------
   STEP 1: IMPORT RAW DATASETS
   --------------------------------------------------------------------------- */
PROC IMPORT DATAFILE="&filepath.Dim_Clients.csv" OUT=WORK.Raw_Clients DBMS=CSV REPLACE; 
    GETNAMES=YES; 
RUN;

PROC IMPORT DATAFILE="&filepath.Dim_Accounts.csv" OUT=WORK.Raw_Accounts DBMS=CSV REPLACE; 
    GETNAMES=YES; 
RUN;

PROC IMPORT DATAFILE="&filepath.Fact_Holdings.csv" OUT=WORK.Raw_Holdings DBMS=CSV REPLACE; 
    GETNAMES=YES; 
RUN;

PROC IMPORT DATAFILE="&filepath.Fact_Transactions.csv" OUT=WORK.Raw_Transactions DBMS=CSV REPLACE; 
    GETNAMES=YES; 
RUN;

/* ---------------------------------------------------------------------------
   STEP 2: TRANSACTION DATA QUALITY CHECKS (ORPHANS & DUPLICATES)
   --------------------------------------------------------------------------- */
/* Check A: Identify Duplicate Transaction IDs */
PROC SORT DATA=WORK.Raw_Transactions OUT=WORK.Sorted_Txns;
    BY TransactionID;
RUN;

DATA WORK.Txn_Valid_Temp WORK.Txn_Duplicates(KEEP=TransactionID AccountID Txn_Type Txn_Amount_USD Txn_Date RCA_Reason);
    SET WORK.Sorted_Txns;
    BY TransactionID;
    LENGTH RCA_Reason $100;
    
    IF FIRST.TransactionID AND LAST.TransactionID THEN OUTPUT WORK.Txn_Valid_Temp;
    ELSE DO;
        RCA_Reason = "ERR_TXN_01: Duplicate Transaction ID Detected";
        OUTPUT WORK.Txn_Duplicates;
    END;
RUN;

/* Check B: Identify Orphan Transactions (AccountID not in Dim_Accounts) */
PROC SQL;
    CREATE TABLE WORK.Txn_Exceptions AS
    SELECT t.TransactionID, t.AccountID, t.Txn_Type, t.Txn_Amount_USD, t.Txn_Date, t.Channel, t.Txn_Status,
           "ERR_TXN_02: Orphan Transaction (Unmapped AccountID)" AS RCA_Reason LENGTH=100
    FROM WORK.Txn_Valid_Temp t
    LEFT JOIN WORK.Raw_Accounts a ON t.AccountID = a.AccountID
    WHERE a.AccountID IS MISSING;

    CREATE TABLE WORK.Txn_Clean AS
    SELECT t.*
    FROM WORK.Txn_Valid_Temp t
    INNER JOIN WORK.Raw_Accounts a ON t.AccountID = a.AccountID;
QUIT;

/* Combine Transaction Exception Records */
DATA WORK.Fact_Txn_Exceptions;
    SET WORK.Txn_Duplicates WORK.Txn_Exceptions;
RUN;

/* ---------------------------------------------------------------------------
   STEP 3: PROC SQL MULTI-TABLE EXTRACT & DIMENSIONAL JOIN
   --------------------------------------------------------------------------- */
PROC SQL;
    CREATE TABLE WORK.Staging_Wealth_Extract AS
    SELECT 
        c.ClientID,
        c.Client_Name,
        c.KYC_Status,
        c.Age,
        c.RM_ID,
        a.AccountID,
        a.Account_Type,
        a.Account_Balance_USD,
        a.Account_Status,
        COALESCE(h.Total_Holdings_USD, 0) AS Total_Holdings_USD,
        COALESCE(t.Total_Txn_Count, 0) AS Total_Txn_Count,
        COALESCE(t.Total_90D_Withdrawals_USD, 0) AS Total_90D_Withdrawals_USD
    FROM WORK.Raw_Clients c
    FULL JOIN WORK.Raw_Accounts a ON c.ClientID = a.ClientID
    LEFT JOIN (
        SELECT AccountID, SUM(Market_Value_USD) AS Total_Holdings_USD
        FROM WORK.Raw_Holdings
        GROUP BY AccountID
    ) h ON a.AccountID = h.AccountID
    LEFT JOIN (
        SELECT 
            AccountID,
            COUNT(TransactionID) AS Total_Txn_Count,
            SUM(CASE WHEN Txn_Type = 'Withdrawal' AND Txn_Date >= (TODAY() - 90) THEN Txn_Amount_USD ELSE 0 END) AS Total_90D_Withdrawals_USD
        FROM WORK.Txn_Clean
        WHERE Txn_Status = 'Completed'
        GROUP BY AccountID
    ) t ON a.AccountID = t.AccountID;
QUIT;

/* ---------------------------------------------------------------------------
   STEP 4: ACCOUNT/CLIENT RULE-BASED DATA QUALITY ROUTING
   --------------------------------------------------------------------------- */
DATA WORK.Client_Account_Clean WORK.Client_Account_Exceptions;
    SET WORK.Staging_Wealth_Extract;
    LENGTH RCA_Reason $100;
    DQ_Flag = 0;

    IF MISSING(ClientID) THEN DO;
        DQ_Flag = 1;
        RCA_Reason = "ERR_ACC_01: Unmapped Account (Missing Client Record)";
    END;
    ELSE IF MISSING(AccountID) THEN DO;
        DQ_Flag = 1;
        RCA_Reason = "ERR_CLI_01: Unmapped Client (No Account Associated)";
    END;
    ELSE IF Account_Balance_USD < 0 THEN DO;
        DQ_Flag = 1;
        RCA_Reason = "ERR_ACC_02: Negative Account Balance Detected";
    END;
    ELSE IF KYC_Status IN ('Pending', 'Rejected') THEN DO;
        DQ_Flag = 1;
        RCA_Reason = "ERR_CLI_02: Client KYC Unverified (Pending/Rejected)";
    END;
    ELSE IF MISSING(Age) THEN DO;
        DQ_Flag = 1;
        RCA_Reason = "ERR_CLI_03: Missing Client Demographic (Age)";
    END;

    IF DQ_Flag = 1 THEN OUTPUT WORK.Client_Account_Exceptions;
    ELSE OUTPUT WORK.Client_Account_Clean;
RUN;

/* ---------------------------------------------------------------------------
   STEP 5: CONTROL-TOTAL RECONCILIATION REPORT
   --------------------------------------------------------------------------- */
PROC SQL;
    CREATE TABLE WORK.Rec_Accounts AS
    SELECT 'Client/Account Records' AS Entity_Type LENGTH=30,
           (SELECT COUNT(*) FROM WORK.Staging_Wealth_Extract) AS Input_Records,
           (SELECT COUNT(*) FROM WORK.Client_Account_Clean) AS Valid_Records,
           (SELECT COUNT(*) FROM WORK.Client_Account_Exceptions) AS Exception_Records,
           ((SELECT COUNT(*) FROM WORK.Client_Account_Clean) + (SELECT COUNT(*) FROM WORK.Client_Account_Exceptions)) AS Total_Accounted
    FROM WORK.Staging_Wealth_Extract (OBS=1);

    CREATE TABLE WORK.Rec_Transactions AS
    SELECT 'Transaction Records' AS Entity_Type LENGTH=30,
           (SELECT COUNT(*) FROM WORK.Raw_Transactions) AS Input_Records,
           (SELECT COUNT(*) FROM WORK.Txn_Clean) AS Valid_Records,
           (SELECT COUNT(*) FROM WORK.Fact_Txn_Exceptions) AS Exception_Records,
           ((SELECT COUNT(*) FROM WORK.Txn_Clean) + (SELECT COUNT(*) FROM WORK.Fact_Txn_Exceptions)) AS Total_Accounted
    FROM WORK.Raw_Transactions (OBS=1);

    CREATE TABLE WORK.Reconciliation_Summary AS
    SELECT * FROM WORK.Rec_Accounts
    UNION ALL
    SELECT * FROM WORK.Rec_Transactions;
QUIT;

TITLE "Data Quality & Reconciliation Audit Summary";
PROC PRINT DATA=WORK.Reconciliation_Summary NOOBS; RUN; TITLE;

/* ---------------------------------------------------------------------------
   STEP 6: CHURN RISK IDENTIFICATION & FINAL DATAMART CREATION
   --------------------------------------------------------------------------- */
DATA WORK.BI_Datamart_Final;
    SET WORK.Client_Account_Clean;
    
    IF Account_Balance_USD > 0 AND Total_90D_Withdrawals_USD > (0.20 * Account_Balance_USD) THEN DO;
        Churn_Risk_Flag = 'High Risk Outflow';
        Churn_Risk_Score = 1;
    END;
    ELSE DO;
        Churn_Risk_Flag = 'Stable';
        Churn_Risk_Score = 0;
    END;
RUN;

/* ---------------------------------------------------------------------------
   STEP 7: EXPORT OUTPUT FILES
   --------------------------------------------------------------------------- */
PROC EXPORT DATA=WORK.BI_Datamart_Final OUTFILE="&filepath.BI_Datamart_Final.csv" DBMS=CSV REPLACE; RUN;
PROC EXPORT DATA=WORK.Client_Account_Exceptions OUTFILE="&filepath.Client_Account_Exceptions.csv" DBMS=CSV REPLACE; RUN;
PROC EXPORT DATA=WORK.Fact_Txn_Exceptions OUTFILE="&filepath.Transaction_Exceptions.csv" DBMS=CSV REPLACE; RUN;
PROC EXPORT DATA=WORK.Reconciliation_Summary OUTFILE="&filepath.Reconciliation_Summary.csv" DBMS=CSV REPLACE; RUN;