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
        print("Initating database")
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
                ON DELETE CASCADE
            )
            """
        create_trackpoint_table_q = """
            CREATE TABLE IF NOT EXISTS trackpoint (
                id int auto_increment PRIMARY KEY,
                activity_id int,
                lat DOUBLE,
                lon DOUBLE,
                altitude int,
                date_days DOUBLE,
                date_time DATETIME,
                CONSTRAINT FK_activity FOREIGN KEY (activity_id)
                REFERENCES activity(id)
                ON DELETE CASCADE
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
        print("adding users")
        users_ids = self.fs_helper.get_all_ids()
        for user_id in users_ids:
            has_label = self.fs_helper.has_labels(user_id)
            q = f"INSERT INTO user (id, has_labels) VALUES ('{user_id}',{has_label})"
            self.cursor.execute(q)
        self.db_connection.commit()

    def truncate_table(self, table_name):
        """ Remove all data in table"""
        self.cursor.execute(f"DELETE {table_name}")
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

    def fetch_10(self, table_name):
        query = "SELECT * FROM %s LIMIT 10"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def add_activity_for_user(self, user_id):
        """ Add activity for a single user """
        plts = self.fs_helper.get_all_plt_by_user_id(user_id)
        for plt in plts:
            with plt.open() as f:
                lines = f.readlines()
                length_of_file = len(lines)
                if length_of_file <= 2500:
                    transportation_mode = "NULL"
                    start_date_time = self.fs_helper.parse_date_time_line(
                        lines[6])
                    end_date_time = self.fs_helper.parse_date_time_line(
                        lines[length_of_file - 1])
                    q = """ INSERT INTO activity (user_id,
                                                  transportation_mode,
                                                  start_date_time,
                                                  end_date_time)
                            VALUES ( '%s', '%s','%s', '%s')
                        """
                    self.cursor.execute(
                        q %
                        (user_id,
                         transportation_mode,
                         start_date_time,
                         end_date_time))
        self.db_connection.commit()

    def add_transportation_mode(self, user_id):
        """Add transportation_mode to existing activites for one user """
        data = self.fs_helper.get_start_time_and_label(user_id)
        if data == [()]:
            return

        for records in data:
            q = """ UPDATE activity
                    SET transportation_mode='%s'
                    WHERE user_id='%s' AND
                          start_date_time='%s'
                """
            query = q % (records[1], user_id, records[0])
            self.cursor.execute(query)

        self.db_connection.commit()

    def add_transportation_mode_all(self):
        """ Add transportation_mode to all users """
        users_ids = self.fs_helper.get_all_ids()
        for user_id in users_ids:
            self.add_transportation_mode(user_id)

    def add_activity_for_all(self):
        """ Add activity for all users"""
        users_ids = self.fs_helper.get_all_ids()
        for user_id in users_ids:
            self.add_activity_for_user(user_id)

    def add_activity(self):
        """ Add activity and transportation_mode for all users"""
        self.add_activity_for_all()
        self.add_transportation_mode_all()

    def add_trackpoints(self):
        """ Add trackpoints for all users"""
        users_ids = self.fs_helper.get_all_ids()
        for user_id in users_ids:
            self.add_trackpoint_by_user_id(user_id)

    def add_trackpoint_by_user_id(self, user_id):
        """ Add trackpoints for a user """
        print("user_id", user_id) # TODO make progress bar
        plts = self.fs_helper.get_all_plt_by_user_id(user_id)
        for plt in plts:
            date_id = self.fs_helper.filename_to_timestamp(plt)
            with plt.open() as f:
                lines = f.readlines()
                length_of_file = len(lines)
                if length_of_file <= 2500:
                    activity_id = self.get_activity_id_by_date(
                        user_id, date_id)
                    q = """ INSERT INTO trackpoint (activity_id,
                                                  lat,
                                                  lon,
                                                  altitude,
                                                  date_days,
                                                  date_time
                                                  ) VALUES
                        """
                    for line in lines[6:]:
                        lat, lon, _, altitude, date_days, date, time = line.split(
                            ',')
                        date_time = date + " " + time.rstrip()
                        values = f"({activity_id}, {lat}, {lon}, {altitude}, {date_days}, '{date_time}'),"
                        q += values
                    # print(q[:-1])
                    self.cursor.execute(q[:-1])

        self.db_connection.commit()

    def get_activity_id_by_date(self, user_id, date_id):
        """ Get activity_id by date and user_id """
        q = f"SELECT id FROM activity WHERE user_id='{user_id}' AND start_date_time='{date_id}'"
        self.cursor.execute(q)
        try:
            activit_id = self.cursor.fetchone()[0]
            return activit_id
        except Exception as e:
            print(q + "\n")


def main():
    """
    Main function
    """
    program = None
    try:
        program = Assignment()
        program.initate_database()
    #    program.add_users()
    #    program.add_activity()
    #    program.add_trackpoints()
        program.fetch_10('user')
        program.fetch_10('activity')
        program.fetch_10('trackpoint')
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
