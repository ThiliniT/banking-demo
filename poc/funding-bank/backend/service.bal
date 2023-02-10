import ballerina/http;
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

# A service representing a network-accessible API
# bound to port `9090`.
# + accountName - Name of Bank Account
# + balance - Current balance of the accoaunt
# + accountId - Account Number

type Accounts record {
    string accountName;
    decimal balance;
    readonly string accountId;
};

table<Accounts> key(accountId) allAccounts = table [
    {accountName: "My Savings Account", balance: 24000.0, accountId: "10001234"},
    {accountName: "College Fund Account", balance: 8572.0, accountId: "10005678"},
    {accountName: "Vacation Account", balance: 7234.0, accountId: "10002222"}
];


mysql:Client dbClient = check new (
    dbHost, dbUser, dbPassword, dbName, dbPort, connectionPool = {maxOpenConnections: 5}
);

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

service / on new http:Listener(9090) {

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
            if (creditDebitIndicator == "Debit" && (<int>accountBalance == 0 || accountBalance < amount))
            {
                return {"message": "Insufficient Balance"};
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

        sql:ParameterizedQuery transactionsQuery = `SELECT * FROM fundingbanktransactions;`;
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

    # A resource for returning the list of accounts
    # + return - The list of accounts
    #
    resource function get accounts() returns json|error {
        // Send a response back to the caller.

        sql:ParameterizedQuery accountsQuery = `SELECT * FROM fundingbankaccounts`;
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
            if(changeAccountBalanceResult is error)
            {
                return changeAccountBalanceResult;
            }
        }
        else {
            issuer = check details.Data.Initiation.CreditorAccount.SchemeName;
            accountId = check details.Data.Initiation.DebtorAccount.Identification;
            error? changeAccountBalanceResult = self.changeAccountBalance(amount, accountId, "Debit");
              if(changeAccountBalanceResult is error)
            {
                return changeAccountBalanceResult;
            }
        }
        return [issuer, accountId];

    }

    private function changeAccountBalance(decimal amount, string accountId, string typeofTrans) returns error?
    {

        sql:ParameterizedQuery accountQuery = `SELECT * FROM fundingbankaccounts WHERE accountId = ${accountId};`;
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
            sql:ParameterizedQuery updatequery = `UPDATE fundingbankaccounts SET balance = ${updatedBalance} WHERE accountId = ${accountId}`;
            sql:ExecutionResult result = check dbClient->execute(updatequery);
        }

    }

    private function getAccountBal(string accountId) returns decimal|error
    {

        io:println("getacoun called  ", accountId);
        sql:ParameterizedQuery accountQuery = `SELECT balance FROM fundingbankaccounts WHERE accountId = ${accountId};`;
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
        sql:ParameterizedQuery transactionQuery = `INSERT INTO fundingbanktransactions (accountId, transactionReference, amount, creditDebitIndicator, bookingDateTime, valueDateTime, issuer, balance, currency ) VALUES (${accountId},  ${transactionReference}, ${amount}, ${creditDebitIndicator} , ${bookingDateTime}, ${valueDateTime}, ${issuer}, ${balance}, ${currency} )`;
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


    # A resource for deleting transactions history and reset accounts
  

    resource function delete records() returns error? {

        sql:ParameterizedQuery query = `Delete from fundingbanktransactions`;
        sql:ExecutionResult result = check dbClient->execute(query);
        query = `Delete from fundingbankaccounts`;
        result = check dbClient->execute(query);
        allAccounts.removeAll();
        allAccounts.add({accountName: "My Savings Account", balance: 24000.0, accountId: "10001234"});
        allAccounts.add({accountName: "College Fund Account", balance: 8572.0, accountId: "10005678"});
        allAccounts.add({accountName: "Vacation Account", balance: 7234.0, accountId: "10002222"});

        foreach Accounts item in allAccounts {
            io:println(item.accountName, item.balance, item.accountId);
            query = `INSERT INTO fundingbankaccounts(accountName, balance, accountId)
                                  VALUES (${item.accountName}, ${item.balance}, ${item.accountId})`;
            result = check dbClient->execute(query);
        }

    }
}
