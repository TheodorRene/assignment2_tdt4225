from sys import argv
from tabulate import tabulate
from DbConnector import DbConnector
class Tasks:
    """
    Main class
    """

    def __init__(self):
        """
        Disse må endres når jeg får tilgang til VMen vår, for øyeblikket kjører jeg det lokalt
        """
        self.connection = DbConnector(
            HOST='tdt4225-19.idi.ntnu.no',
            DATABASE='db',
            USER='testbruker',
            PASSWORD=argv[1]
        )
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def task1(self):
        def count_table(table_name):
            q_count_table = f"SELECT '{table_name}', count(id) FROM {table_name}"
            self.cursor.execute(q_count_table)
            rows = self.cursor.fetchall()
            print(tabulate(rows, headers=self.cursor.column_names))

        count_table("user")
        count_table("activity")
        count_table("trackpoint")

    def task5(self):
        query = "SELECT COUNT(DISTINCT transportation_mode) FROM activity"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

        query = "SELECT COUNT(id), transportation_mode FROM activity WHERE transportation_mode != 'NULL' GROUP BY transportation_mode ORDER BY COUNT(id) DESC"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

def main():
    program = Tasks()
    program.task1()
    program.task5()

if __name__ == '__main__':
    main()

