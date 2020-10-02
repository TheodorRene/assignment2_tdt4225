from sys import argv
from tabulate import tabulate
from DbConnector import DbConnector
class Tasks:
    """
    Main class
    """

    def __init__(self):
        """ Sets up database connection """
        self.connection = DbConnector(
            HOST='tdt4225-19.idi.ntnu.no',
            DATABASE='db',
            USER='testbruker',
            PASSWORD=argv[1]
        )
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def task2(self):
        """ Find the average activities per user. """

        def count_table(table_name):
            """ Return number of instances in a given table"""

            q_count_table = f"SELECT count(id) FROM {table_name}"
            self.cursor.execute(q_count_table)
            rows = self.cursor.fetchall()
            return rows[0][0]

        activity_count = count_table("activity")
        user_count = count_table("user")

        print(activity_count/user_count)

    def task6a(self):
        """ Find the year with the most recorded activities. """
        query = "SELECT YEAR(start_date_time) AS year, " \
                "COUNT(id) AS activities " \
                "FROM activity " \
                "GROUP BY year " \
                "ORDER BY activities DESC;"

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task6b(self):
        """ Is the year with the most activities the year with the most recorded hours? """

        year_minute_table = "SELECT YEAR(start_date_time) AS year, " \
                            "TIMESTAMPDIFF(MINUTE, start_date_time, end_date_time) as minutes " \
                            "FROM activity "

        query = "SELECT year, SUM(minutes) as total_minutes " \
                f"FROM ({year_minute_table}) AS year_minute_table " \
                "GROUP BY year " \
                "ORDER BY total_minutes DESC;"

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = Tasks()
    program.task2()
    program.task6a()
    program.task6b()

if __name__ == '__main__':
    main()

