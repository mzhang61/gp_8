import time
import mysql.connector

# Database Configuration
DB_HOST = "127.0.0.1"
DB_USER = "root"
DB_PASSWORD = "new_password"
DB_NAME = "gp_8"


def connect_to_db():
    """# Code Optimized: Uses try-except for robust error handling and returns the connection."""
    try:
        return mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
    except mysql.connector.Error as err:
        print(f"❌ Database connection failed: {err}")
        return None  # Ensures safe handling in case of failure


def fetch_distinct_employee_ids(cursor):
    """# Code Optimized: Uses a set for efficient lookup and removes duplicates."""
    cursor.execute("SELECT DISTINCT EmployeeId FROM survey_result WHERE EmployeeId IS NOT NULL")
    return {row[0] for row in cursor.fetchall()}  # Converts result into a set for faster lookups


def fetch_employee_details(cursor, employee_id):
    """# Code Optimized: Fetches all required fields in one query to reduce database calls."""
    cursor.execute("SELECT Id, FirstName, LastName FROM employee_name WHERE Id = %s", (employee_id,))
    return cursor.fetchone()  # Returns a single row with Employee details


def fetch_survey_values(cursor, employee_id):
    """# Code Optimized: Fetches both AbsentEmployeeReason & Certifications in a single query."""
    query = """
        SELECT AttributeId, ValueCode 
        FROM survey_result 
        WHERE EmployeeId = %s AND AttributeId IN ('AbsentEmployeeReason', 'Certifications')
    """
    cursor.execute(query, (employee_id,))
    results = dict(cursor.fetchall())  # Converts list of tuples into a dictionary
    return results.get("AbsentEmployeeReason"), results.get("Certifications")  # Returns optimized lookup


def process_survey_data():
    """# Code Optimized: Uses high-precision timing, efficient queries, and bulk inserts."""

    start_time = time.perf_counter()  # # Code Optimized: Uses time.perf_counter() for better precision

    with connect_to_db() as connection:  # # Code Optimized: Uses 'with' to ensure automatic connection cleanup
        if not connection:
            print("❌ Failed to connect to the database. Exiting...")
            return

        with connection.cursor() as cursor:  # # Code Optimized: Uses 'with' for automatic cursor cleanup
            try:
                distinct_employees = fetch_distinct_employee_ids(cursor)
                insert_data = []

                print("\n🔍 Processing Employee Records...")

                for employee_id in distinct_employees:
                    print(f"🔹 Processing Employee ID: {employee_id}")

                    emp_details = fetch_employee_details(cursor, employee_id)
                    if not emp_details:
                        print(f"⚠️ Employee ID {employee_id} not found in employee_name table. Skipping...")
                        continue

                    emp_id, first_name, last_name = emp_details
                    absent_reason, certifications = fetch_survey_values(cursor, employee_id)

                    insert_data.append((emp_id, first_name, last_name, absent_reason, certifications))
                    print(f" ✅ {first_name} {last_name} | Absent: {absent_reason}, Certs: {certifications}")

                print(f"\n✅ Total distinct employees processed: {len(insert_data)}")

                # Step 5: Empty survey_report table
                cursor.execute("DELETE FROM survey_report")

                # Step 6: Bulk insert processed data into survey_report
                insert_query = """
                    INSERT INTO survey_report (EmployeeId, FirstName, LastName, AbsentEmployeeReason, Certifications)
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_query,
                                   insert_data)  # # Code Optimized: Uses executemany() for batch insertion
                connection.commit()
                print("\n🎉 Survey report updated successfully.")

            except mysql.connector.Error as err:
                print(f"❌ Error: {err}")
                connection.rollback()  # # Code Optimized: Ensures rollback in case of failure

    execution_time_ms = (time.perf_counter() - start_time) * 1000  # # Code Optimized: Uses high-precision timing
    print(f"⏳ Execution Time: {execution_time_ms:.2f} ms")


if __name__ == "__main__":
    print("🚀 Starting the script...")
    process_survey_data()
    print("✅ Script execution completed.")