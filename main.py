import pandas as pd
import json
import datetime

df = pd.read_csv("data.csv", sep = "|")
rows = df.shape[0]
print(df)

# Parse JSON
def parse(data):
    j1 = json.loads(data)
    return j1

# Convert TimeStamp to DateTime
def convert_ts_dt(ts):
    date = datetime.datetime.fromtimestamp(ts / 1e3)
    return date

# Function 1 - Not dealing with missing values
def get_balances(LoanID, BankReport):
    """
    Function that takes LoanID and BankReport as arguments, and gives 5 lists: LoanID, AccountID,
    AccountNumber, BalanceOpen, BalanceClose - that need to be populated into a DataFrame.
    """
    for account in parse(BankReport)['accounts']:
        if(account['accountType'] == "checking"):
            try:
                acc_id = int(account['accountId'])
                acc_number = account['accountNumber']
                df_temp = pd.DataFrame(account['transactions'])
                df_temp["postedDate"] = df_temp["postedDate"].apply(convert_ts_dt)
                df_temp.sort_values(by = "postedDate", inplace=True)
                open_bal = df_temp.head(1)['balance']; close_bal = df_temp.tail(1)['balance'];
                indexer = open_bal.index[0]; open_bal = open_bal.loc[indexer]; indexer = close_bal.index[0]; close_bal = close_bal.loc[indexer];
                bal_open.append(open_bal); bal_close.append(close_bal); loan_ids.append(int(LoanID));
                acc_ids.append(acc_id); acc_numbers.append(acc_number);
                """return pd.DataFrame({
                        "Loan ID": int(LoanID), "Account ID": acc_id, "Account number": acc_number,
                        "Balance open": open_bal, "Balance close": close_bal
                        }, index=[0])"""
            except KeyError:
                pass
            finally:
                # print("\n---\n\nLoanID: {}, \nAccountID: {}, \nAccountNumber{}, \nBalanceOpen: {}, \nBalanceClose: {}".format(loan_ids, acc_ids, acc_numbers, bal_open, bal_close))
                pass

# Initialize the 5 lists to be used to create the DataFrame
bal_open = []; bal_close = []; loan_ids = []; acc_ids = []; acc_numbers = [];

# Function calls for all rows
for row_index in range(rows):
    get_balances(df["LoanId"][row_index], df["BankReportData"][row_index])

# Create DatFrame
df_final = pd.DataFrame({
    "Loan IDs": loan_ids, "Account ID": acc_ids, "Account Number": acc_numbers,
    "Balance open": bal_open, "Balance close": bal_close
})
df_final

# Function 2 - Dealing with missing values
def get_balances_remove_nans(LoanID, BankReport):
    """
    Function that takes LoanID and BankReport as arguments, and gives 5 lists: LoanID, AccountID,
    AccountNumber, BalanceOpen, BalanceClose - that need to be populated into a DataFrame.
    """
    for account in parse(BankReport)['accounts']:
        if(account['accountType'] == "checking"):
            try:
                acc_id = int(account['accountId'])
                acc_number = account['accountNumber']
                df_temp = pd.DataFrame(account['transactions'])
                df_temp["postedDate"] = df_temp["postedDate"].apply(convert_ts_dt)
                df_temp.sort_values(by = "postedDate", inplace=True)
                open_bal = df_temp['balance'].dropna().head(1); close_bal = df_temp['balance'].dropna().tail(1); # The change
                indexer = open_bal.index[0]; open_bal = open_bal.loc[indexer]; indexer = close_bal.index[0]; close_bal = close_bal.loc[indexer];
                bal_open.append(open_bal); bal_close.append(close_bal); loan_ids.append(int(LoanID));
                acc_ids.append(acc_id); acc_numbers.append(acc_number);
            except KeyError:
                pass
            finally:
                # print("\n---\n\nLoanID: {}, \nAccountID: {}, \nAccountNumber{}, \nBalanceOpen: {}, \nBalanceClose: {}".format(loan_ids, acc_ids, acc_numbers, bal_open, bal_close))
                pass

# Initialize the 5 lists to be used to create the DataFrame
bal_open = []; bal_close = []; loan_ids = []; acc_ids = []; acc_numbers = [];

# Function calls for all rows
for row_index in range(rows):
    get_balances_remove_nans(df["LoanId"][row_index], df["BankReportData"][row_index])

# Create DatFrame
df_final_remove_nans = pd.DataFrame({
    "Loan IDs": loan_ids, "Account ID": acc_ids, "Account Number": acc_numbers,
    "Balance open": bal_open, "Balance close": bal_close
})
df_final_remove_nans