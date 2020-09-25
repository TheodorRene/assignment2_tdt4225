"""
File for assignmetn
"""
from sys import argv
from tabulate import tabulate
from DbConnector import DbConnector
from traverse import FileTraversal


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
        self.fs_helper = FileTraversal()


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
                id int auto_increment PRIMARY KEY,
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
                id int auto_increment PRIMARY KEY,
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

    def add_users(self):
        """ Adds all the users into the database """
        users_ids = self.fs_helper.get_all_ids()
        for user_id in users_ids:
            has_label = self.fs_helper.has_labels(user_id)
            q = f"INSERT INTO user (id, has_labels) VALUES ('{user_id}',{has_label})"
            self.cursor.execute(q)
        self.db_connection.commit()

    def truncate_table(self, table_name):
        self.cursor.execute(f"TRUNCATE {table_name}")
        self.db_connection.commit()
        print(f"Truncated {table_name} table")

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def add_activity_for_user(self,user_id):
       plts = self.fs_helper.get_all_plt_by_user_id(user_id)
       for plt in plts:
            with plt.open()as f:
                lines = f.readlines()
                length_of_file = len(lines)
                if length_of_file <= 2500:
                    transportation_mode = "NULL"
                    start_date_time = self.parse_date_time(lines[6])
                    end_date_time = self.parse_date_time(lines[length_of_file - 1])
                    q = """ INSERT INTO activity (user_id,
                                                  transportation_mode,
                                                  start_date_time,
                                                  end_date_time)
                            VALUES ( '%s', '%s','%s', '%s')
                        """
                    self.cursor.execute(q % (user_id, transportation_mode, start_date_time, end_date_time))
       self.db_connection.commit()


    def parse_date_time(self, timestamp_line):
        return timestamp_line.split(',')[-2] + " " + timestamp_line.split(",")[-1]







def main():
    """
    Main function
    """
    program = None
    try:
        program = Assignment()
        #program.initate_database()
        #program.add_users()
        program.add_activity_for_user('000')
        program.fetch_data('activity')
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
