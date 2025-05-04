import pytz
from datetime import datetime
from connection import getconnection, release_connection
import sqlalchemy

def track_pdf_daily_usage(pdf_name):
    """
    Tracks PDF usage on a daily basis
    Creates a new entry if the PDF + date combination doesn't exist,
    or increments the counter if it does exist.
    
    Args:
        pdf_name (str): Name of the PDF being accessed
    
    Returns:
        bool: True if operation was successful, False otherwise
    """
    if not pdf_name:
        return False
        
    connection = getconnection()
    try:
        # Get current date in Indian timezone
        india_tz = pytz.timezone('Asia/Kolkata')
        current_datetime = datetime.now(india_tz)
        current_date = current_datetime.date()
        
        with connection.connect() as conn:
            # Check if PDF entry for today exists
            result = conn.execute(
                sqlalchemy.text("""
                SELECT * FROM pdf_daily_tracker 
                WHERE pdf_name = :pdf_name AND query_date = :query_date
                """),
                {"pdf_name": pdf_name, "query_date": current_date}
            ).fetchone()
            
            if result:
                # Update counter for today
                conn.execute(
                    sqlalchemy.text("""
                    UPDATE pdf_daily_tracker 
                    SET counter = counter + 1
                    WHERE pdf_name = :pdf_name AND query_date = :query_date
                    """),
                    {"pdf_name": pdf_name, "query_date": current_date}
                )
            else:
                # Insert new record for today
                conn.execute(
                    sqlalchemy.text("""
                    INSERT INTO pdf_daily_tracker (pdf_name, query_date, counter) 
                    VALUES (:pdf_name, :query_date, 1)
                    """),
                    {"pdf_name": pdf_name, "query_date": current_date}
                )
            
            conn.commit()
            return True
    except Exception as e:
        print(f"Error tracking daily PDF usage: {e}")
        return False
    finally:
        release_connection(connection)
