import ballerina/http;
import ballerina/uuid;
import ballerina/time;
import ballerina/log;

int transactionIndex = 0;

type Transactions record {|

    string accountId;
    readonly int transactionId;
    string transactionReference;
    float amount;
    string creditDebitIndicator;
    string bookingDateTime;
    string valueDateTime;
    string issuer;
    float balance;
    string currency;
|};

table<Transactions> key(transactionId) allTransactions = table [
    ];

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
    float balance;

|};

table<Accounts> key(accountId) allAccounts = table [
    ];

type AmountRec record {|
    float amount;
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
            json createdAccountDetails = {"AccountID": accountNumber, "Account Name": accountName, "Status": "Enabled", "StatusUpdateDateTime": time:utcToString(time:utcNow()), "Currency": currency, "AccountType": accountType, "AccountSubType": accountSubType, "Nickname": nickName, "OpeningDate": openingDate, "MaturityDate": maturityDate, "Balance": 0};
            allAccounts.add({accountId: accountNumber, accountName: accountName, status: "Enabled", statusUpdateDateTime: time:utcToString(time:utcNow()), currency: currency, nickName: nickName, openingDate: openingDate, maturityDate: maturityDate, accountType: accountType, accountSubType: accountSubType, balance: 0});
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
        } on fail var e {
            string message = e.message();
            log:printError(message);
            return http:BAD_REQUEST;
        }

    }

    # A resource for creating new payment records
    # + paymentDetails - the payment resource
    # + return - payment information
    resource function post payments(@http:Payload json paymentDetails) returns json|http:BadRequest {
        // Send a response back to the caller.
        do {
            // Send a response back to the caller.
            string reference = check paymentDetails.Data.Initiation.Reference;
            string creditDebitIndicator = check paymentDetails.Data.Initiation.CreditDebitIndicator;
            string amountTemp = check paymentDetails.Data.Initiation.Amount.Amount;
            string currency = check paymentDetails.Data.Initiation.Amount.Currency;
            string bookingDateTime = time:utcToString(time:utcNow());
            string valueDateTime = time:utcToString(time:utcNow());
            float amount = check float:fromString(amountTemp);
            string[] accountId_issuer = check self.setAccount(paymentDetails, amount);
            string issuer = accountId_issuer[0];
            string accountId = accountId_issuer[1];
            transactionIndex += 1;
            float accountBalance = check self.getAccountBal(accountId);
            return self.setCredit(accountId, transactionIndex, reference, amount, creditDebitIndicator, bookingDateTime, valueDateTime, issuer, accountBalance, currency);
        } on fail var e {
            string message = e.message();
            log:printError(message);
            return http:BAD_REQUEST;
        }

    }

    # A resource for returning transaction records
    # + return - transaction history
    #
    resource function get transactions() returns json {
        // Send a response back to the caller.
        json[] transactionsHistory = <json[]>allTransactions.toJson();
        return {
            "Data": {
                "Transaction": [transactionsHistory]
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
        json[] listOfAccounts = <json[]>allAccounts.toJson();
        json returnData = {"Data": {"Account": listOfAccounts}};
        return returnData;
    }

    private function setAccount(json details, float amount) returns string[]|error
    {
        string creditDebitIndicator = check details.Data.Initiation.CreditDebitIndicator;
        string issuer = "";
        string accountId = "";
        if (creditDebitIndicator == "Credit")
        {
            issuer = check details.Data.Initiation.DebtorAccount.SchemeName;
            accountId = check details.Data.Initiation.CreditorAccount.Identification;
            self.changeAccountBalance(amount, accountId, "Credit");

        }
        else {
            issuer = check details.Data.Initiation.CreditorAccount.SchemeName;
            accountId = check details.Data.Initiation.DebtorAccount.Identification;
            self.changeAccountBalance(amount, accountId, "Debit");

        }
        return [issuer, accountId];

    }

    private function changeAccountBalance(float amount, string accountId, string typeofTrans)
    {
        foreach Accounts ac in allAccounts {
            if (ac.accountId == accountId && typeofTrans == "Credit")
            {
                ac.balance += amount;
            }
            if (ac.accountId == accountId && typeofTrans == "Debit")
            {
                ac.balance -= amount;
            }

        }
    }

    private function getAccountBal(string accountId) returns float|error
    {
        foreach Accounts ac in allAccounts {
            if (ac.accountId == accountId)
            {
                return ac.balance;
            }

        }

        return 0;
    }

    private function setCredit(string accountId, int transactionId, string transactionReference,
            float amount,
            string creditDebitIndicator,
            string bookingDateTime,
            string valueDateTime,
            string issuer,
            float balance,
            string currency) returns json {

        allTransactions.add({accountId: accountId, transactionId: transactionIndex, transactionReference: transactionReference, amount: amount, creditDebitIndicator: creditDebitIndicator, bookingDateTime: bookingDateTime, valueDateTime: valueDateTime, issuer: issuer, balance: balance, currency: currency});
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

    }
    # A resource for deleting accounts and transactions
    resource function delete records() {
        allTransactions.removeAll();
        allAccounts.removeAll();

    }
}

