import mariadb


class MariaDBClient:
    def __init__(self, host, user, password, database, port=3306):
        try:
            self.connection = mariadb.connect(
                host=host, user=user, password=password, database=database, port=port
            )
            self.cursor = self.connection.cursor()
            print("✅ Connected to MariaDB")
        except mariadb.Error as e:
            print(f"❌ Connection error: {e}")

    def read_query(self, query, params=None):
        try:
            self.cursor.execute(query, params or ())
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except mariadb.Error as e:
            print(f"❌ Read error: {e}")
            return []

    def write_query(self, query, params=None):
        try:
            self.cursor.execute(query, params or ())
            self.connection.commit()
            print("✅ Write successful.")
        except mariadb.Error as e:
            print(f"❌ Write error: {e}")
            self.connection.rollback()

    def close(self):
        self.cursor.close()
        self.connection.close()
        print("🔌 Connection closed.")


# Set Terminal Encoding to UTF-8 (Linux/macOS)
# If you're using Linux or macOS, ensure your terminal supports UTF-8:

# bash
# $ export PYTHONIOENCODING=utf-8

# Or, run your script like this:

# bash
# $ PYTHONIOENCODING=utf-8 python your_script.py
