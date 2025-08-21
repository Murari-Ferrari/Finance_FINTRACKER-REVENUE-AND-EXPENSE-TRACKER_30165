import psycopg2
from psycopg2 import sql
import os
import uuid
from decimal import Decimal

# Function to establish a database connection
def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=os.environ.get("DB_NAME", "fintracker"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", "99Mur@ri99"),
            host=os.environ.get("DB_HOST", "localhost")
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

# --- DDL (Data Definition Language) ---
def create_table():
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id VARCHAR(255) PRIMARY KEY,
                    transaction_date DATE NOT NULL,
                    description TEXT,
                    amount DECIMAL(10, 2) NOT NULL,
                    type VARCHAR(20) -- 'Revenue' or 'Expense'
                );
            """)
            conn.commit()
            print("Table 'transactions' created or already exists.")
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
        finally:
            cursor.close()
            conn.close()

# --- CRUD (Create, Read, Update, Delete) Operations ---
def create_transaction(transaction_date, description, amount, type):
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            transaction_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO transactions (transaction_id, transaction_date, description, amount, type)
                VALUES (%s, %s, %s, %s, %s);
            """, (transaction_id, transaction_date, description, amount, type))
            conn.commit()
            return "Transaction added successfully."
        except psycopg2.Error as e:
            conn.rollback()
            return f"Error adding transaction: {e}"
        finally:
            cursor.close()
            conn.close()

def read_transactions(transaction_type=None, sort_by='transaction_date', sort_order='DESC'):
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            query = sql.SQL("SELECT * FROM transactions")
            params = []
            
            if transaction_type and transaction_type != 'All':
                query += sql.SQL(" WHERE type = %s")
                params.append(transaction_type)
            
            # Sanitize sort_by and sort_order to prevent SQL injection
            valid_sort_columns = ['transaction_date', 'amount']
            valid_sort_orders = ['ASC', 'DESC']
            
            if sort_by not in valid_sort_columns:
                sort_by = 'transaction_date'
            if sort_order not in valid_sort_orders:
                sort_order = 'DESC'
            
            query += sql.SQL(" ORDER BY {} {}").format(
                sql.Identifier(sort_by), sql.SQL(sort_order)
            )

            cursor.execute(query, params)
            transactions = cursor.fetchall()
            return transactions
        except psycopg2.Error as e:
            print(f"Error reading transactions: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

def update_transaction(transaction_id, transaction_date, description, amount, type):
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE transactions
                SET transaction_date = %s, description = %s, amount = %s, type = %s
                WHERE transaction_id = %s;
            """, (transaction_date, description, amount, type, transaction_id))
            conn.commit()
            return "Transaction updated successfully."
        except psycopg2.Error as e:
            conn.rollback()
            return f"Error updating transaction: {e}"
        finally:
            cursor.close()
            conn.close()

def delete_transaction(transaction_id):
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM transactions WHERE transaction_id = %s;", (transaction_id,))
            conn.commit()
            return "Transaction deleted successfully."
        except psycopg2.Error as e:
            conn.rollback()
            return f"Error deleting transaction: {e}"
        finally:
            cursor.close()
            conn.close()

# --- Business Insights & Aggregation ---
def get_total_transactions():
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM transactions;")
            count = cursor.fetchone()[0]
            return count
        except psycopg2.Error as e:
            print(f"Error getting total count: {e}")
            return 0
        finally:
            cursor.close()
            conn.close()

def get_total_revenue():
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT SUM(amount) FROM transactions WHERE type = 'Revenue';")
            total = cursor.fetchone()[0]
            return total if total is not None else Decimal(0)
        except psycopg2.Error as e:
            print(f"Error getting total revenue: {e}")
            return Decimal(0)
        finally:
            cursor.close()
            conn.close()

def get_total_expense():
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT SUM(amount) FROM transactions WHERE type = 'Expense';")
            total = cursor.fetchone()[0]
            return total if total is not None else Decimal(0)
        except psycopg2.Error as e:
            print(f"Error getting total expense: {e}")
            return Decimal(0)
        finally:
            cursor.close()
            conn.close()

def get_net_income():
    total_revenue = get_total_revenue()
    total_expense = get_total_expense()
    
    if total_revenue is not None and total_expense is not None:
        net_income = total_revenue - total_expense
        return net_income
    else:
        return Decimal(0)