import pymysql
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access environment variables
DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "database": os.getenv("DB_NAME"),
}

def create_connection():
    return pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
    )

def test_db_connection():
    try:
        conn = create_connection()
        conn.close()
        return True
    except Exception as e:
        print("Connection Error:", e)
        return False

# Test the database connection
if test_db_connection():
    print("Database connection test successful.")
else:
    print("Database connection test failed.")

def calculate_percentage_appointments():
    try:
        conn = create_connection()
        
        query = """
            SELECT COUNT(*) AS total_appointments,
                   SUM(CASE WHEN HOUR(date_created) >= 17 OR HOUR(date_created) < 9 THEN 1 ELSE 0 END) AS late_night_appointments
            FROM yosi_emr.emr_appointment
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        
        total_appointments = result[0]
        late_night_appointments = result[1]
        
        cursor.close()
        conn.close()
        
        percentage = (late_night_appointments / total_appointments) * 100
        return total_appointments, late_night_appointments, percentage
    
    except Exception as e:
        print("Error:", e)
        return None, None, None

# Calculate the total number of appointments, late night appointments, and the percentage
if test_db_connection():
    total_appointments, late_night_appointments, percentage = calculate_percentage_appointments()

    if total_appointments is not None and late_night_appointments is not None and percentage is not None:
        print(f"Total number of appointments: {total_appointments}")
        print(f"Number of late night appointments: {late_night_appointments}")
        print(f"The percentage of appointments booked between 5pm and 9am is: {percentage:.2f}%")
