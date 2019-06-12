import pandas as pd
import json
import datetime
import numpy as np # For the function calls, as they include indexing

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

# Function 1
# Function that takes rows (by index) and returns 5 lists
bal_open = []; bal_close = []; loan_ids = []; acc_ids = []; acc_numbers = [];
def take_index_return_balances(row_index):
    """
    Function that takes Index of row as argument, and gives 5 lists: LoanID, AccountID,
    AccountNumber, BalanceOpen, BalanceClose - that need to be populated into a DataFrame.
    """
    # Takes length of dict for each of the "accounts"
    length_of_account_indices = len(pd.DataFrame(df.BankReportData.apply(parse)[row_index])['accounts'])
    for account_index in range(length_of_account_indices):
        # Condition to see if accountType == "checking"; If yes, perform some actions, else pass
        if(pd.DataFrame(df.BankReportData.apply(parse)[row_index])['accounts'][account_index]['accountType'] == "checking"):
            try:
                acc_id = int((df.BankReportData.apply(parse)[row_index])['accounts'][account_index]["accountId"])
                acc_number = (df.BankReportData.apply(parse)[row_index])['accounts'][account_index]['accountNumber']
                df_temp = pd.DataFrame(pd.DataFrame(df.BankReportData.apply(parse)[row_index])['accounts'][account_index]['transactions'])
                df_temp["postedDate"] = df_temp["postedDate"].apply(convert_ts_dt)
                df_temp.sort_values(by = "postedDate", inplace=True)
                open_bal = df_temp.head(1)['balance']; close_bal = df_temp.tail(1)['balance'];
                indexer = open_bal.index[0]; open_bal = open_bal.loc[indexer]; indexer = close_bal.index[0]; close_bal = close_bal.loc[indexer];
                bal_open.append(open_bal); bal_close.append(close_bal); loan_ids.append(int(df.LoanId[row_index]));
                acc_ids.append(acc_id); acc_numbers.append(acc_number);
            except KeyError:
                pass
            finally:
                # print("\nLoadID: {}; RowIndex: {}; AccountIndexForThatRow: {}".format(int(df.LoanId[row_index]), row_index, account_index))
                pass
        else:
            pass
# End of function

# Function call for all rows ("run_the_function" variable is useless; Just meant to populate the 3 lists)
# run_the_function = [take_index_return_balances(item) for item in range(rows)]

# Function call - Using apply
run_the_function_using_apply = pd.Series(np.arange(0, rows)).apply(take_index_return_balances)

# Desired DataFrame (This dataframe has NaNs)
df_final = pd.DataFrame({
    "Loan ID": loan_ids, "Account ID": acc_ids, "Account number": acc_numbers,
    "Balance open": bal_open, "Balance close": bal_close
})
df_final

# Function 2
# Same function as above, but at has one line of changes; Removes NaNs
""" Accounting for missing values for Balance column (NaNs), and removing them """
# Function that takes rows (by index) and returns 5 lists
bal_open = []; bal_close = []; loan_ids = []; acc_ids = []; acc_numbers = [];
def take_index_return_balances_remove_rows_with_missing_values(row_index):
    """
    Function that takes Index of row as argument, and gives 5 lists: LoanID, AccountID,
    AccountNumber, BalanceOpen, BalanceClose - that need to be populated into a DataFrame.
    """
    # Takes length of dict for each of the "accounts"
    length_of_account_indices = len(pd.DataFrame(df.BankReportData.apply(parse)[row_index])['accounts'])
    for account_index in range(length_of_account_indices):
        # Condition to see if accountType == "checking"; If yes, perform some actions, else pass
        if(pd.DataFrame(df.BankReportData.apply(parse)[row_index])['accounts'][account_index]['accountType'] == "checking"):
            try:
                acc_id = int((df.BankReportData.apply(parse)[row_index])['accounts'][account_index]["accountId"])
                acc_number = (df.BankReportData.apply(parse)[row_index])['accounts'][account_index]['accountNumber']
                df_temp = pd.DataFrame(pd.DataFrame(df.BankReportData.apply(parse)[row_index])['accounts'][account_index]['transactions'])
                df_temp["postedDate"] = df_temp["postedDate"].apply(convert_ts_dt)
                df_temp.sort_values(by = "postedDate", inplace=True)
                open_bal = df_temp['balance'].dropna().head(1); close_bal = df_temp['balance'].dropna().tail(1); # The change
                indexer = open_bal.index[0]; open_bal = open_bal.loc[indexer]; indexer = close_bal.index[0]; close_bal = close_bal.loc[indexer];
                bal_open.append(open_bal); bal_close.append(close_bal); loan_ids.append(int(df.LoanId[row_index]));
                acc_ids.append(acc_id); acc_numbers.append(acc_number);
            except KeyError:
                pass
            finally:
                # print("\nLoadID: {}; RowIndex: {}; AccountIndexForThatRow: {}".format(int(df.LoanId[row_index]), row_index, account_index))
                pass
        else:
            pass
# End of function

# Function call for all rows ("run_the_function_2" variable is useless; Just meant to populate the 3 lists)
# run_the_function_2 = [take_index_return_balances_remove_rows_with_missing_values(item) for item in range(rows)]

# Function call - Using apply
run_the_function_2_using_apply = pd.Series(np.arange(0, rows)).apply(take_index_return_balances_remove_rows_with_missing_values)

# Desired DataFrame? (Without NaNs)
df_final_removed_nan = pd.DataFrame({
    "Loan IDs": loan_ids, "Account ID": acc_ids, "Account Number": acc_numbers,
    "Balance open": bal_open, "Balance close": bal_close
})
df_final_removed_nan