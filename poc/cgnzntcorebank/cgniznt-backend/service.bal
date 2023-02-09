import ballerina/http;
import ballerina/uuid;
import ballerina/time;
import ballerina/log;
import ballerinax/mysql.driver as _;
import ballerinax/mysql;
import ballerina/sql;
import ballerina/io;

configurable string dbHost = ?;
configurable string dbUser = ?;
configurable string dbPassword = ?;
configurable string dbName = ?;
configurable int dbPort = ?;

type Transactions record {|

    string accountId;
    readonly int transactionId;
    string transactionReference;
    decimal amount;
    string creditDebitIndicator;
    string bookingDateTime;
    string valueDateTime;
    string issuer;
    decimal balance;
    string currency;
|};

table<Transactions> key(transactionId) allTransactions = table [
    ];

mysql:Client dbClient = check new (
    dbHost, dbUser, dbPassword, dbName, dbPort, connectionPool = {maxOpenConnections: 5}
);



type Accounts record {|
    readonly string accountId;
    string accountName;
    string status;
    string statusUpdateDateTime;
    string currency;
    string nickName;
    string openingDate;
    string maturityDate;
    string accountType;
    string accountSubType;
    decimal balance;

|};

table<Accounts> key(accountId) allAccounts = table [
    ];

type AmountRec record {|
    decimal amount;
    string currency;
|};

# A service representing a network-accessible API
# bound to port `9090`.
service / on new http:Listener(9090) {

    # A resource for creating a new account
    # + accountDetails - json containing the new account details
    # + return - newly created account information.
    resource function post create\-account(@http:Payload json accountDetails) returns json|http:BadRequest {
        // Send a response back to the caller.

        do {
            // Send a response back to the caller.

            string accountName = check accountDetails.Data.Account.DisplayName;
            string currency = check accountDetails.Data.Account.Currency;
            string nickName = check accountDetails.Data.Account.Nickname;
            string openingDate = check accountDetails.Data.Account.OpeningDate;
            string maturityDate = check accountDetails.Data.Account.MaturityDate;
            string accountType = check accountDetails.Data.Account.AccountType;
            string accountSubType = check accountDetails.Data.Account.AccountSubType;
            string accountNumber = uuid:createType4AsString();
            string statusUpdateDateTime = time:utcToString(time:utcNow());
            json createdAccountDetails = {"AccountID": accountNumber, "Account Name": accountName, "Status": "Enabled", "StatusUpdateDateTime": statusUpdateDateTime, "Currency": currency, "AccountType": accountType, "AccountSubType": accountSubType, "Nickname": nickName, "OpeningDate": openingDate, "MaturityDate": maturityDate, "Balance": 0.0};
            //allAccounts.add({accountId: accountNumber, accountName: accountName, status: "Enabled", statusUpdateDateTime: time:utcToString(time:utcNow()), currency: currency, nickName: nickName, openingDate: openingDate, maturityDate: maturityDate, accountType: accountType, accountSubType: accountSubType, balance: 0.0});
            sql:ParameterizedQuery accountQuery = `INSERT INTO cognizantbankaccounts (accountId, accountName, status, nickName, openingDate, maturityDate, currency, accountType, accountSubType, balance, statusUpdateDateTime ) VALUES (${accountNumber}, ${accountName}, "Enabled", ${nickName}, ${openingDate}, ${maturityDate}, ${currency}, ${accountType}, ${accountSubType}, 0, ${statusUpdateDateTime}  )`;
            sql:ExecutionResult result = check dbClient->execute(accountQuery);

            if (result.affectedRowCount == 1) {
                json returnvalue = {
                    "Data": {
                        "Account": [createdAccountDetails],
                        "Meta": {
                        },
                        "Risk": {
                        },
                        "Links": {
                            "Self": ""
                        }
                    }
                };

                return returnvalue;
            } else {
                return {"Message": "Failure in creating the account"};
            }

        } on fail var e {
            string message = e.message();
            log:printError(message);
            return http:BAD_REQUEST;
        }

    }

    # A resource for creating new payment records
    # + paymentDetails - the payment resource
    # + return - payment information
    resource function post payments(@http:Payload json paymentDetails) returns json|http:BadRequest|error {
        // Send a response back to the caller.
        do {
            // Send a response back to the caller.
            string reference = check paymentDetails.Data.Initiation.Reference;
            string creditDebitIndicator = check paymentDetails.Data.Initiation.CreditDebitIndicator;
            string amountTemp = check paymentDetails.Data.Initiation.Amount.Amount;
            string currency = check paymentDetails.Data.Initiation.Amount.Currency;
            string bookingDateTime = time:utcToString(time:utcNow());
            string valueDateTime = time:utcToString(time:utcNow());
            decimal amount = check decimal:fromString(amountTemp);
            string[] accountId_issuer = check self.setAccount(paymentDetails, amount);

            string issuer = accountId_issuer[0];
            string accountId = accountId_issuer[1];

            decimal accountBalance = check self.getAccountBal(accountId);
            if(creditDebitIndicator=="Debit" && (<int>accountBalance==0||accountBalance<amount) )
            {
                return {"message":"Insufficient Balance"};
            }
            return self.setCredit(accountId, reference, amount, creditDebitIndicator, bookingDateTime, valueDateTime, issuer, accountBalance, currency);
        } on fail var e {
            string message = e.message();
            log:printError(message);
            return http:BAD_REQUEST;
        }

    }

    # A resource for returning transaction records
    # + return - transaction history
    #
    resource function get transactions() returns json|error {
        // Send a response back to the caller.

        sql:ParameterizedQuery transactionsQuery = `SELECT * FROM cognizantbanktransactions;`;
        stream<Transactions, sql:Error?> transactionsStream = dbClient->query(transactionsQuery);
        allTransactions.removeAll();
        check from Transactions trsctn in transactionsStream
            do {
                allTransactions.add(trsctn);

            };

        return {
            "Data": {
                "Transaction": [allTransactions.toJson()]
            }
        };

    }

    # A resource for returning the list of funding banks 
    # + return - The list of funding banks information
    resource function get registered\-funding\-banks() returns json|error {
        // Send a response back to the caller.
        json listOfBanks = {"ListofBanks": ["ABC", "Bank GSA", "ERGO Bank", "MIS Bank", "LP Bank", "Co Bank"]};
        return listOfBanks;
    }

    # A resource for returning the list of accounts
    # + return - The list of accounts
    #
    resource function get accounts() returns json|error {
        // Send a response back to the caller.

        sql:ParameterizedQuery accountsQuery = `SELECT * FROM cognizantbankaccounts;`;
        stream<Accounts, sql:Error?> accountsStream = dbClient->query(accountsQuery);
        allAccounts.removeAll();
        check from Accounts acnts in accountsStream
            do {
                allAccounts.add(acnts);

            };

        return {
            "Data": {
                "Account": [allAccounts.toJson()]
            }
        };

    }

    private function setAccount(json details, decimal amount) returns string[]|error
    {
        string creditDebitIndicator = check details.Data.Initiation.CreditDebitIndicator;
        string issuer = "";
        string accountId = "";
        if (creditDebitIndicator == "Credit")
        {
            issuer = check details.Data.Initiation.DebtorAccount.SchemeName;
            accountId = check details.Data.Initiation.CreditorAccount.Identification;
            io:println(amount, accountId);
            error? changeAccountBalanceResult = self.changeAccountBalance(amount, accountId, "Credit");

        }
        else {
            issuer = check details.Data.Initiation.CreditorAccount.SchemeName;
            accountId = check details.Data.Initiation.DebtorAccount.Identification;
            error? changeAccountBalanceResult = self.changeAccountBalance(amount, accountId, "Debit");

        }
        return [issuer, accountId];

    }

    private function changeAccountBalance(decimal amount, string accountId, string typeofTrans) returns error?
    {

        sql:ParameterizedQuery accountQuery = `SELECT * FROM cognizantbankaccounts WHERE accountId = ${accountId};`;
        stream<Accounts, sql:Error?> accountsStream = dbClient->query(accountQuery);
        decimal updatedBalance = 0.0;
        check from Accounts acct in accountsStream
            do {
                if (acct.accountId == accountId && typeofTrans == "Credit")
                {
                    updatedBalance = acct.balance + amount;
                }
                if (acct.accountId == accountId && typeofTrans == "Debit")
                {
                    updatedBalance = acct.balance - amount;
                    if (<float>updatedBalance < 0.0)
                    {
                        updatedBalance = 0;
                    }
                }

            };
            sql:Error?? close = accountsStream.close();
        if (<float>updatedBalance >= 0.0)
        {
            sql:ParameterizedQuery updatequery = `UPDATE cognizantbankaccounts SET balance = ${updatedBalance} WHERE accountId = ${accountId}`;
            sql:ExecutionResult result = check dbClient->execute(updatequery);
        }

    }

    private function getAccountBal(string accountId) returns decimal|error
    {

        io:println("getacoun called  ", accountId);
        sql:ParameterizedQuery accountQuery = `SELECT balance FROM cognizantbankaccounts WHERE accountId = ${accountId};`;
        decimal balance = check dbClient->queryRow(accountQuery);

        io:println(balance);
        return balance;
    }

    private function setCredit(string accountId, string transactionReference,
            decimal amount,
            string creditDebitIndicator,
            string bookingDateTime,
            string valueDateTime,
            string issuer,
            decimal balance,
            string currency) returns json|http:BadRequest {

        //allTransactions.add({accountId: accountId, transactionId: transactionIndex, transactionReference: transactionReference, amount: amount, creditDebitIndicator: creditDebitIndicator, bookingDateTime: bookingDateTime, valueDateTime: valueDateTime, issuer: issuer, balance: balance, currency: currency});

        io:println(amount, balance);
        sql:ParameterizedQuery transactionQuery = `INSERT INTO cognizantbanktransactions (accountId, transactionReference, amount, creditDebitIndicator, bookingDateTime, valueDateTime, issuer, balance, currency ) VALUES (${accountId},  ${transactionReference}, ${amount}, ${creditDebitIndicator} , ${bookingDateTime}, ${valueDateTime}, ${issuer}, ${balance}, ${currency} )`;
        do {

            sql:ExecutionResult result = check dbClient->execute(transactionQuery);

            json transactionSummary = {
                "Data": {
                    "Status": "InitiationCompleted",
                    "StatusUpdateDateTime": bookingDateTime,
                    "CreationDateTime": valueDateTime,
                    "Initiation": {
                        "Issuer": issuer
                    },
                    "Reference": transactionReference,
                    "Amount": {
                        "Amount": amount,
                        "Currency": currency
                    }
                },
                "Meta": {

                },
                "Links": {
                    "Self": ""
                }
            };

            return transactionSummary;
        } on fail var e {
            string message = e.message();
            log:printError(message);
            return http:BAD_REQUEST;
        }

    }

    # A resource for deleting accounts and transactions
    resource function delete records() returns error? {

        sql:ParameterizedQuery query = `Delete from cognizantbankaccounts`;
        sql:ExecutionResult result = check dbClient->execute(query);
        query = `Delete from cognizantbanktransactions`;
        result = check dbClient->execute(query);

    }
}
