
import pymysql

class MySQLDatabase:
    def __init__(self, host, user, password, dbname):
        """
        Initialize the database connection.
        Args:
        host (str): Database host address.
        user (str): Username for the database.
        password (str): Password for the database.
        dbname (str): Name of the database to connect to.
        """
        self.connection = pymysql.connect(host=host,
                                          user=user,
                                          password=password,
                                          db=dbname,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.connection.cursor()

    # 插入数据
    def insert_data(self, sql):
        """
        Insert data into the database based on the SQL query.
        Args:
        sql (str): SQL query string.
        data (dict): Dictionary containing data to insert.
        """
        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except Exception as e:
            print(f"Error executing query: {e}")
    def fetch_data(self, sql):
        """
        Fetch data from the database based on the SQL query.
        Args:
        sql (str): SQL query string.
        Returns:
        list: A list of dictionaries representing rows returned by the query.
        """
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return result
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def __del__(self):
        """
        Destructor to ensure the database connection is closed.
        """
        self.cursor.close()
        self.connection.close()

# Usage
if __name__ == "__main__":
    # Example usage
    db = MySQLDatabase('43.138.111.201', 'root', 'PKL.19881001', 'llm_product')
    sql_query = "SELECT * FROM products"
    data = db.fetch_data(sql_query)
    print(data)