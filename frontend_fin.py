import streamlit as st
import pandas as pd
from backend_fin import *

# Set page title
st.set_page_config(page_title="Finance: Revenue & Expense Tracker", layout="wide")

st.title("30165_Finance: Revenue & Expense Tracker")
st.markdown("### A quick overview of the company's financial health.")

# Ensure the database table exists
create_table()

# --- Aggregation and Business Insights Section ---
st.header("Business Insights")

col1, col2, col3, col4 = st.columns(4)

total_transactions = get_total_transactions()
total_revenue = get_total_revenue()
total_expense = get_total_expense()
net_income = get_net_income()

with col1:
    st.metric(label="Total Transactions", value=total_transactions)
with col2:
    st.metric(label="Total Revenue", value=f"${total_revenue:,.2f}")
with col3:
    st.metric(label="Total Expenses", value=f"${total_expense:,.2f}")
with col4:
    st.metric(label="Net Income", value=f"${net_income:,.2f}")

st.markdown("---")

# --- CRUD Operations Section (Create) ---
st.header("Add New Transaction")
with st.form("new_transaction_form", clear_on_submit=True):
    date = st.date_input("Transaction Date")
    description = st.text_area("Description")
    amount = st.number_input("Amount", min_value=0.01, format="%.2f")
    transaction_type = st.selectbox("Type", ["Revenue", "Expense"])
    submit_button = st.form_submit_button("Add Transaction")
    if submit_button:
        response = create_transaction(date, description, amount, transaction_type)
        st.success(response)

st.markdown("---")

# --- Read & Filtering & Sorting Section ---
st.header("All Transactions")

col_filter, col_sort = st.columns(2)

with col_filter:
    selected_type = st.selectbox("Filter by Type", ["All", "Revenue", "Expense"])

with col_sort:
    sort_option = st.selectbox("Sort by", ["transaction_date", "amount"])
    sort_order = st.radio("Order", ["Descending", "Ascending"])
    sort_order_db = "DESC" if sort_order == "Descending" else "ASC"

transactions_data = read_transactions(selected_type, sort_option, sort_order_db)

if transactions_data:
    df = pd.DataFrame(transactions_data, columns=['transaction_id', 'transaction_date', 'description', 'amount', 'type'])
    # Optional: Format the amount column for better readability
    df['amount'] = df['amount'].apply(lambda x: f"${x:,.2f}")
    df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.strftime('%Y-%m-%d')
    st.dataframe(df.set_index('transaction_id'))
else:
    st.info("No transactions found.")