"""
File for assignmetn
"""
from sys import argv
from tabulate import tabulate
from DbConnector import DbConnector


class Assignment:
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

    def initate_database(self):
        """
        Initates the schema
        """
        create_user_table_q = """
            CREATE TABLE IF NOT EXISTS user (
                id varchar(255) PRIMARY KEY,
                has_labels boolean
            )
            """
        create_activity_table_q = """
            CREATE TABLE IF NOT EXISTS activity (
                id int PRIMARY KEY,
                user_id varchar(255),
                transportation_mode varchar(255),
                start_date_time DATETIME,
                end_date_time DATETIME,
                CONSTRAINT FK_user FOREIGN KEY (user_id)
                REFERENCES user(id)
            )
            """
        create_trackpoint_table_q = """
            CREATE TABLE IF NOT EXISTS trackpoint (
                id int PRIMARY KEY,
                activity_id int,
                lat DOUBLE,
                lon DOUBLE,
                date_days DOUBLE,
                date_time DATETIME,
                CONSTRAINT FK_activity FOREIGN KEY (activity_id)
                REFERENCES activity(id)
            )
            """
        self.cursor.execute(create_user_table_q)
        self.cursor.execute(create_activity_table_q)
        self.cursor.execute(create_trackpoint_table_q)
        self.db_connection.commit()

    def show_tables(self):
        """
        Display tables in a neat manner
        """
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    """
    Main function
    """
    program = None
    try:
        program = Assignment()
        program.initate_database()
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
