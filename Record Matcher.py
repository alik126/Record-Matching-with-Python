import sys
import time
import traceback
import pandas as pd
from fuzzywuzzy import process
from tkinter import filedialog, Tk
from combined_dict_old import recon_dict

flattened_recon_dict = {}


def select_file(title="Select a file"):
    root = Tk()
    root.withdraw()  # This hides the root window
    file_path = filedialog.askopenfilename(title=title)
    return file_path


def load_data(deposit_file, bank_statement_file):
    try:
        # Read EFT Transactions sheet
        eft_transactions = pd.read_excel(deposit_file, sheet_name='EFT Transactions', skiprows=0)
        # Read Non-EFT Transactions sheet
        non_eft_transactions = pd.read_excel(deposit_file, sheet_name='Non-EFT Transactions', skiprows=0)

        # Convert 'Date' to datetime and format as DD/MM/YYYY string
        eft_transactions['Date'] = (pd.to_datetime(eft_transactions['Date'], errors='coerce', dayfirst=True).
                                    dt.strftime('%d/%m/%Y'))
        non_eft_transactions['Date'] = (pd.to_datetime(non_eft_transactions['Date'], errors='coerce', dayfirst=True).
                                        dt.strftime('%d/%m/%Y'))

        # Combine both DataFrames
        deposit_entries = pd.concat([eft_transactions, non_eft_transactions], ignore_index=True)
        deposit_entries.loc[:, deposit_entries.columns != 'Remarks'] = (
            deposit_entries.loc[:, deposit_entries.columns != 'Remarks'].
            apply(lambda x: x.str.lower() if x.dtype == 'object' else x))

        # Read bank_statement_file
        transactions = pd.read_excel(bank_statement_file, skiprows=4, header=0, usecols="A:F", dtype={'Reference': str})
        transactions.drop([0, 1, 2], inplace=True)
        transactions.reset_index(drop=True, inplace=True)
        transactions['Date'] = pd.to_datetime(transactions['Date'], format='%d %b %Y', errors='coerce')
        transactions['Date'] = transactions['Date'].apply(lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else '')
        transactions = transactions.map(lambda x: x.lower() if isinstance(x, str) else x)

    except Exception as e:
        print("An error occurred while loading data:", e)
        traceback.print_exc()
        return None, None, None, None

    return deposit_entries, transactions, eft_transactions, non_eft_transactions


def replace_values(value):
    # Check if value is not a float
    if not isinstance(value, float):
        value_lower = value.lower()
        for category, descriptions in recon_dict.items():
            for description in descriptions:
                flattened_recon_dict[description.lower()] = category.lower()

        # Check if value exists in flattened_recon_dict keys
        if value_lower in flattened_recon_dict:
            return flattened_recon_dict[value_lower]
    return value


def preprocess_data(deposit_entries, transactions):
    try:
        deposit_entries['Payer_Cat'] = deposit_entries['Payer'].apply(lambda x: replace_values(x))
        deposit_entries['Standardized_Name'] = deposit_entries['Name'].apply(lambda x: replace_values(x))
        transactions['Bank_Cat'] = deposit_entries['Payer_Cat']

        # Handle 'Cheque #' conversion and numeric conversions
        deposit_entries['Cheque #'] = deposit_entries['Cheque #'].astype(str).str.replace('.0', '', regex=False)
        deposit_entries['Cheque #'] = deposit_entries['Cheque #'].apply(lambda x: x[:5] if pd.notnull(x) else '')
        deposit_entries['Amount'] = pd.to_numeric(deposit_entries['Amount'], errors='coerce')
        transactions['Debit'] = pd.to_numeric(transactions['Debit'], errors='coerce')
        transactions['Credit'] = pd.to_numeric(transactions['Credit'], errors='coerce')

    except Exception as e:
        print("An error occurred in preprocess_data:", e)
        traceback.print_exc()

    return deposit_entries, transactions


def compare_substrings(name, description):
    if isinstance(name, float) or isinstance(description, float):
        return False

    # Extract the first 6 characters from both name and description
    name_substring = name[:6].lower()
    description_substring = description[:6].lower()

    # Compare the extracted substrings using fuzzy matching
    score, best = process.extractOne(name_substring, description_substring)

    # Return True if similarity score is above threshold, else False
    similarity_threshold = 30  # Adjust as needed
    return best >= similarity_threshold


def format_date(date):
    try:
        return pd.to_datetime(date, dayfirst=True, errors='coerce').strftime('%d/%m/%Y')
    except ValueError:
        return '' if pd.isna(date) or date == '' else date


def match_transactions(deposit_entries, transactions):
    amount_tolerance = 0.01
    potential_matches = pd.DataFrame()  # Initialize an empty DataFrame to store potential matches
    matched_details = []  # Collect detailed info for matched transactions
    unmatched_deposit_entries = []  # Deposits that couldn't be matched
    unmatched_transactions = []  # Transactions that couldn't be matched
    matched_indices = set()  # Keep track of matched transaction indices
    matched_details_df = pd.DataFrame()  # Initialize as empty DataFrame
    unmatched_deposit_entries_df = pd.DataFrame()  # Initialize as empty DataFrame
    unmatched_transactions_df = pd.DataFrame()  # Initialize as empty DataFrame
    for dep_index, deposit_row in deposit_entries.iterrows():
        for index, transaction_row in transactions.iterrows():

            formatted_bank_date = format_date(deposit_row['Date'])

            amount_filter = None
            sum_filter = None

            if deposit_row['Remarks'] == 'Single':
                condition1 = (transaction_row['Debit'] != 0)
                condition2 = (deposit_row['Amount'] != 0)
                condition3 = (transaction_row['Debit'] >= deposit_row['Amount'] * (1 - amount_tolerance))
                condition4 = (transaction_row['Debit'] <= deposit_row['Amount'] * (1 + amount_tolerance))
                condition5 = compare_substrings(transaction_row['Bank_Cat'], deposit_row['Payer_Cat'])
                condition6 = compare_substrings(transaction_row['Description'], deposit_row['Standardized_Name'])
                amount_filter = condition1 and condition2 and condition3 and condition4 and condition5 and condition6

            elif deposit_row['Remarks'] == 'Sum':
                condition1 = (deposit_row['Sum'] != 0)
                condition2 = (transaction_row['Debit'] != 0)
                condition3 = (transaction_row['Debit'] >= deposit_row['Sum'] * (1 - amount_tolerance))
                condition4 = (transaction_row['Debit'] <= deposit_row['Sum'] * (1 + amount_tolerance))
                condition5 = pd.isna(transaction_row['Date']) or (transaction_row['Date'] == formatted_bank_date)
                condition6 = compare_substrings(transaction_row['Bank_Cat'], deposit_row['Payer_Cat'])
                condition7 = compare_substrings(transaction_row['Description'], deposit_row['Standardized_Name'])
                sum_filter = (condition1 and condition2 and condition3 and condition4 and condition5 and
                              condition6 and condition7)

            if amount_filter or sum_filter:  # Single boolean value True
                potential_matches = potential_matches._append(transaction_row, ignore_index=True)

        if potential_matches.empty:
            remarks = str(deposit_row.get('Remarks', '')).strip()
            if remarks in ['Single', 'Sum']:
                unmatched_deposit_entries.append({
                    'Deposit_Date': format_date(deposit_row['Date']),
                    'Depo Payer': deposit_row['Payer'],
                    'Payer Cat': deposit_row['Payer'],
                    'Depo_Name': deposit_row['Name'],
                    'Depo Cheque #': deposit_row['Cheque #'],
                    'Depo_Amt': deposit_row['Amount'],
                    'Remarks': remarks
                })

        # Process matched deposits
        for trans_idx, trans_row in potential_matches.iterrows():
            if trans_row.get('Debit', 0) != 0 and trans_idx not in matched_indices:
                match_detail = {
                    'Depo Payer': deposit_row['Payer'],
                    'Payer Cat': deposit_row['Payer'],
                    'Deposit_Date': format_date(deposit_row['Date']),
                    'Bank_Date': format_date(trans_row['Date']),
                    'Depo_Bank': deposit_row['Bank'],
                    'Depo_Name': deposit_row['Name'],
                    'Bank_Description': trans_row['Description'],
                    'Bank_Cat': trans_row['Bank_Cat'],
                    'Bank_Reference': trans_row['Reference'],
                    'Depo Cheque #': deposit_row['Cheque #'],
                    'Depo_Amt': deposit_row['Amount'],
                    'Bank_Debit': trans_row.get('Debit', 0),
                    'Sum': deposit_row.get('Sum', 0),
                    'Rec_Bank': trans_idx,
                    'Remarks': ''
                }
                matched_details.append(match_detail)
                matched_indices.add(trans_idx)

        # Process unmatched transactions
        for trans_idx, trans_row in transactions.iterrows():
            if trans_idx not in matched_indices and trans_row['Debit'] != 0:
                unmatched_transactions.append({
                    'Bank_Date': format_date(trans_row['Date']),
                    'Bank_Description': trans_row['Description'],
                    'Bank_Cat': trans_row['Bank_Cat'],
                    'Bank_Reference': trans_row['Reference'],
                    'Bank_Debit': trans_row['Debit'],
                    'Bank_Credit': trans_row['Credit'],
                    'Rec_Bank': trans_idx,
                    'Remarks': ''
                })

    # Convert lists to DataFrames
    matched_details_df = pd.DataFrame(matched_details)
    unmatched_deposit_entries_df = pd.DataFrame(unmatched_deposit_entries)
    unmatched_transactions_df = pd.DataFrame(unmatched_transactions)

    # Return the DataFrames for further use if needed
    return matched_details_df, unmatched_deposit_entries_df, unmatched_transactions_df


def generate_reports(matched_details_df, unmatched_deposit_entries_df, unmatched_transactions_df):
    try:
        # Use ExcelWriter with the openpyxl engine
        with pd.ExcelWriter('reports.xlsx', engine='openpyxl') as writer:
            # Matched Details Report
            matched_details_df.to_excel(writer, index=False, sheet_name='Matched Details')

            # Unmatched Deposit Entries Report
            unmatched_deposit_entries_df.to_excel(writer, index=False, sheet_name='Unmatched Deposit Entries')

            # Unmatched Transactions Report
            unmatched_transactions_df.to_excel(writer, index=False, sheet_name='Unmatched Transactions')

        print("Reports generated successfully: reports.xlsx")
    except Exception as e:
        print("An error occurred while generating the reports:", e)


def main():
    try:
        # Select deposit file
        print("========>>>>.. select deposit file")
        deposit_file = select_file("Please Load the Deposit xlsx file:")
        print("Deposit file selected:", deposit_file)

        # Select bank statement file
        print("========>>>>..select bank statement file")
        bank_statement_file = select_file("Please Load the Bank Statement xlsx file:")
        print("Bank statement file selected:", bank_statement_file)

        start = time.time()
        start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
        print('Starting Time ------->', start)

        deposit_entries, transactions, _, _ = load_data(deposit_file, bank_statement_file)
        if deposit_entries is not None and transactions is not None:
            deposit_entries, transactions = preprocess_data(deposit_entries, transactions)
            matched_details_df, unmatched_deposit_entries_df, unmatched_transactions_df = (
                match_transactions(deposit_entries, transactions))

            # Generate reportsS
            generate_reports(matched_details_df, unmatched_deposit_entries_df, unmatched_transactions_df)

    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()

    finally:
        end = time.time()
        end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
        print('Ending Time ------->', end)
        print("Cleaning up resources...")
        # get_ipython().run_line_magic('reset', '-sf')
        sys.exit()


if __name__ == "__main__":
    main()
