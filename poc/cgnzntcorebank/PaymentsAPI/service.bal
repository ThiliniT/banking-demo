import ballerina/http;
import cgnzntpoc/cognizantcorebankbackend_no;




//import ballerina/io;

float accountBalance = 0;
int transactionIndex = 0;

type Transactions record {|
    readonly int indexNo;
    float amount;
    string sourceOfPayment;
|};

table<Transactions> key(indexNo) allTransactions = table [
    ];

# A service representing a network-accessible API
# bound to port `9090`.
service / on new http:Listener(9090) {

    # A resource for updating the accountBalance and maintaining transactions
    # + return - Status of transaction/History of Transactions
    resource function put creditAccount(float amount, string sourceOfPayment) returns string {
        // Send a response back to the caller.
        accountBalance += amount;
        transactionIndex += 1;
        allTransactions.add({indexNo: transactionIndex, amount: amount, sourceOfPayment: sourceOfPayment}); //update the transaction History table
        return "Transaction Successful";

    }
    resource function put debitAccount(float amount, string sourceOfPayment) returns string|error {
        // Send a response back to the caller.
        cognizantcorebankbackend_no:Client endEp = check new (config = {
            auth: {
                clientId: "",
                clientSecret: ""
            }
        });
        json[] test = check endEp->/records.delete
        
        accountBalance -= amount;
        transactionIndex += 1;
        allTransactions.add({indexNo: transactionIndex, amount: amount, sourceOfPayment: sourceOfPayment});

        return "Transaction Successful";

    }
    resource function get transactions() returns json[] {
        // Send a response back to the caller.
        json[] transactionsHistory = <json[]>allTransactions.toJson();
        return transactionsHistory;

    }

}
