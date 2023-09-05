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






########## Calculate the percentage of appointments booked between 5pm and 9am ##########
def calculate_percentage_appointments():
    try:
        conn = create_connection()
        
        query = """
            SELECT COUNT(*) AS total_appointments,
                   SUM(CASE WHEN HOUR(date_created) >= 17 OR HOUR(date_created) < 9 THEN 1 ELSE 0 END) AS late_night_appointments
            FROM yosi_emr.emr_appointment
            WHERE date_created >= '2022-07-01' AND date_created < '2023-08-01'
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






########## Calculate the percentage of appointments booked during weekends ##########
def calculate_percentage_weekend_appointments():
    try:
        conn = create_connection()  # Create a database connection

        query = """
            SELECT COUNT(*) AS total_appointments,
                   SUM(CASE WHEN DAYOFWEEK(date_created) = 1 OR DAYOFWEEK(date_created) = 7 THEN 1 ELSE 0 END) AS weekend_appointments
            FROM yosi_emr.emr_appointment
            WHERE date_created >= '2022-07-01' AND date_created < '2023-08-01';
        """

        cursor = conn.cursor()  # Create a cursor object to execute SQL queries
        cursor.execute(query)  # Execute the SQL query
        result = cursor.fetchone()  # Fetch the query result

        total_appointments = result[0]  # Extract the total appointments count
        weekend_appointments = result[1]  # Extract the weekend appointments count

        cursor.close()  # Close the cursor
        conn.close()  # Close the database connection

        if total_appointments > 0:
            percentage = (weekend_appointments / total_appointments) * 100  # Calculate the percentage of weekend appointments
            return total_appointments, weekend_appointments, percentage  # Return the results
        else:
            print("No appointments available in the specified date range.")
            return None, None, None  # Return None values when there are no appointments

    except Exception as e:
        print("Error:", e)  # Print an error message if an exception occurs
        return None, None, None  # Return None values in case of an error

# Calculate the percentage of weekend appointments in the specified date range
if test_db_connection():
    total_appointments, weekend_appointments, percentage = calculate_percentage_weekend_appointments()

    if total_appointments is not None and weekend_appointments is not None and percentage is not None:
        print(f"Total number of appointments: {total_appointments}")
        print(f"Number of weekend appointments: {weekend_appointments}")
        print(f"The percentage of appointments booked during weekends between July 2022 and July 2023 is: {percentage:.2f}%")








########## Calculate the average intake form completion time ##########
def calculate_intake_form_completion_time():
    try:
        conn = create_connection()  # Create a database connection
        
        query = """
            SELECT TIMESTAMPDIFF(SECOND, yosi_paper_work_date, yosi_checkin_date) AS intake_form_completion_time
            FROM yosi_emr.emr_invite
            WHERE (yosi_paper_work_date != '0000-00-00 00:00:00' AND yosi_checkin_date != '0000-00-00 00:00:00')
            AND (
                (yosi_paper_work_date BETWEEN '2022-07-01 00:00:00' AND '2023-07-31 23:59:59')
                AND (yosi_checkin_date BETWEEN '2022-07-01 00:00:00' AND '2023-07-31 23:59:59')
                );
        """
        
        cursor = conn.cursor()  # Create a cursor object to execute SQL queries
        cursor.execute(query)  # Execute the SQL query
        result = cursor.fetchone()  # Fetch the query result
        
        if result is not None and result[0] is not None:
            intake_form_completion_time_seconds = result[0]  # Extract the intake form completion time in seconds
            
            cursor.close()  # Close the cursor
            conn.close()  # Close the database connection
            
            # Convert the time difference from seconds to a more readable format
            minutes, seconds = divmod(intake_form_completion_time_seconds, 60)
            hours, minutes = divmod(minutes, 60)
            days, hours = divmod(hours, 24)
            
            return days, hours, minutes, seconds  # Return the formatted intake form completion time
        else:
            print("No data available to calculate intake form completion time.")
            return None, None, None, None  # Return None values when there's no data
    
    except Exception as e:
        print("Error:", e)  # Print an error message if an exception occurs
        return None, None, None, None  # Return None values in case of an error

# Calculate the intake form completion time
if test_db_connection():
    days, hours, minutes, seconds = calculate_intake_form_completion_time()

    if days is not None and hours is not None and minutes is not None and seconds is not None:
        print(f"Average intake form completion time: {days} days, {hours} hours, {minutes} minutes, {seconds} seconds")









########## Calculate survey completion rate ##########
def calculate_survey_completion_rate():
    try:
        conn = create_connection()  # Create a database connection

        query = """
            SELECT 
                SUM(CASE WHEN survey_completed_on != '0000-00-00 00:00:00' THEN 1 ELSE 0 END) AS completed_surveys,
                COUNT(*) AS total_surveys
                FROM yosi_emr.post_visit_survey_invites
                WHERE survey_sent_on >= '2022-07-01' AND survey_sent_on < '2023-08-01'
        """

        cursor = conn.cursor()  # Create a cursor object to execute SQL queries
        cursor.execute(query)  # Execute the SQL query
        result = cursor.fetchone()  # Fetch the query result

        completed_surveys = result[0]  # Extract the number of completed surveys
        total_surveys = result[1]  # Extract the total number of surveys

        cursor.close()  # Close the cursor
        conn.close()  # Close the database connection

        if total_surveys > 0:
            completion_rate = (completed_surveys / total_surveys) * 100  # Calculate the completion rate
            return completed_surveys, total_surveys, completion_rate  # Return the results
        else:
            print("No surveys available to calculate completion rate.")
            return None, None, None  # Return None values when there are no surveys

    except Exception as e:
        print("Error:", e)  # Print an error message if an exception occurs
        return None, None, None  # Return None values in case of an error

# Calculate the survey completion rate
if test_db_connection():
    completed_surveys, total_surveys, completion_rate = calculate_survey_completion_rate()

    if completed_surveys is not None and total_surveys is not None and completion_rate is not None:
        print(f"Number of completed surveys: {completed_surveys}")
        print(f"Total number of surveys: {total_surveys}")
        print(f"Survey completion rate: {completion_rate:.2f}%")
