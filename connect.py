import sqlite3

try:
    with sqlite3.connect("weather.db") as conn:
        print(f"Opened SQLite database with version {sqlite3.sqlite_version} successfully.")
        cursor = conn.cursor()
        
        # create the database
        cursor.execute("CREATE TABLE weather_data (city VARCHAR(50) NOT NULL, state VARCHAR(50) NOT NULL, country VARCHAR(50) NOT NULL, lat FLOAT(10, 7) NOT NULL DEFAULT 0.0, long FLOAT(10, 7) NOT NULL DEFAULT 0.0, weather TEXT NOT NULL DEFAULT '', timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (city, state, country))")
        res = cursor.execute("SELECT name FROM sqlite_master")
        print(res.fetchone())

except sqlite3.OperationalError as e:
    print("Failed to open database:", e)