import time
import mysql.connector

# 1. Establish a database connection to the MYSQL database.
DB_HOST = "127.0.0.1"
DB_USER = "root"
DB_PASSWORD = "new_password"
DB_NAME = "gp_8"

def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host = DB_HOST,
            user = DB_USER,
            password = DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            print("Database connected successfully")
            return connection
    except mysql.connector.Error as err:
        print(f"Database connection failed: {err}")
        return None

# 2. get the distinct id from table survey_result
def fetch_distinct_employee_ids(cursor):
    query = "SELECT DISTINCT EmployeeId FROM survey_result WHERE EmployeeId IS NOT NULL"
    cursor.execute(query)
    distinctEmployees = [row[0] for row in cursor.fetchall()]
    return distinctEmployees

# 4a. use distinct EmployeeId to get employee Id on employee_name table
def fetch_employee_details(cursor, employee_id):
    query = "SELECT Id, FirstName, LastName FROM employee_name where Id = %s"
    cursor.execute(query, (employee_id,))
    return cursor.fetchone()
# 4b-e
def fetch_survey_value(cursor, employee_id, attribute_id):
    query = "SELECT ValueCode From survey_result WHERE EmployeeId = %s AND AttributeId = %s"
    cursor.execute(query, (employee_id, attribute_id))
    result = cursor.fetchone()
    return result[0] if result else None

def process_survey_data():
    # Start timing
    start_time = time.time()
    connection = connect_to_db()
    if connection is None:
        print("Failed to connect to the database. Exiting...")
        return
    cursor = connection.cursor()

    try:
        # step 2
        distinct_employees = fetch_distinct_employee_ids(cursor)
        # store data
        insert_data = []

        count = 0
        # get distinct id from survey_result_table, distinct id is employee_name table id
        for employee_id in distinct_employees:
            # print distinct id
            print(f"\n Employee ID: {employee_id}")

            # 4a. get emp_name table info by id
            emp_details = fetch_employee_details(cursor, employee_id)
            if emp_details is None:
                print(f"Employee ID {employee_id} not found in employee_name table. ")
                continue
            emp_id, first_name, last_name = emp_details
            print(f" Employee Name: {first_name} {last_name}")

            # 4b: Fetch AbsentEmployeeReason
            absent_reason = fetch_survey_value(cursor, employee_id, "AbsentEmployeeReason")
            # 4c: Fetch Certifications
            certifications = fetch_survey_value(cursor, employee_id, "Certifications")
            # 4E: COMBINE and store data
            record = (emp_id, first_name, last_name, absent_reason, certifications)
            insert_data.append(record)
            print(f" Absent Reason: {absent_reason}, Certifications: {certifications}")
        print(f" The total number of distinct id from survey_result is {len(insert_data)}")
        # step 5: Empty survey_report table
        cursor.execute("DELETE FROM survey_report")

        # step 6: Insert processed data into survey_report
        insert_query = """
            INSERT INTO survey_report (EmployeeId, FirstName, LastName, AbsentEmployeeReason, Certifications)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, insert_data)
        connection.commit()
        print("\n Survey report updated sucessfully.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
        print("Database connection closed.")

        # End timing
        end_time = time.time()
        execution_time_ms = (end_time - start_time) * 1000
        print(f"Execution Time: {execution_time_ms:.2f} ms")



if __name__ == "__main__":
    print("Starting the script...")
    process_survey_data()
    print("Script execution completed.")
    
