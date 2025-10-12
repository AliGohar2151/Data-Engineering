import mysql.connector
import mariadb


class MariaDBClient:
    def __init__(self, host, user, password, database, port=3306):
        try:
            self.connection = mariadb.connect(
                host=host, user=user, password=password, database=database, port=port
            )
            self.cursor = self.connection.cursor()
            print("Successfully connected to MariaDB")

        except mariadb.Error as e:
            print(f"Error connecting to MariaDB: {e}")

    def read_query(self, query, params=None):
        try:
            self.cursor.execute(query, params or ())
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except mariadb.Error as e:
            print(f"Error reading data: {e}")
            return []

    def write_query(self, query, params=None):
        try:
            self.cursor.execute(query, params or ())
            self.connection.commit()
            print("Write successful.")
        except mariadb.Error as e:
            print(f"Error writing data: {e}")
            self.connection.rollback()

    def close(self):
        self.cursor.close()
        self.connection.close()
        print("Connection closed.")
