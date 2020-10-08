from sys import argv
from tabulate import tabulate
from DbConnector import DbConnector
from haversine import haversine


class Tasks:

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

    def task_1(self):
        """ How many users, activities and trackpoints are there in the dataset? """

        def count_table(table_name):
            q_count_table = f"SELECT '{table_name}', COUNT(id) FROM {table_name}"
            self.cursor.execute(q_count_table)
            rows = self.cursor.fetchall()
            print(tabulate(rows, headers=self.cursor.column_names))

        count_table("user")
        count_table("activity")
        count_table("trackpoint")

    def task_2(self):
        """ Find the average activities per user. """

        user_activity_count = """
                                SELECT user.id, COUNT(activity.id) AS activity_count
                                FROM user INNER JOIN activity
                                ON user.id = activity.user_id
                                GROUP BY user.id
                              """

        query = f"""
                    SELECT AVG(activity_count)
                    FROM ({user_activity_count}) AS user_activity_count
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_3(self):
        """ Find the top 20 users with the highest number of activities. """

        query = """
                    SELECT user_id, COUNT(user_id) AS num_of_activities
                    FROM activity 
                    GROUP BY(user_id) 
                    ORDER BY num_of_activities DESC 
                    LIMIT 20
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_4(self):
        """ Find all users who have taken a taxi. """
        query = """
                    SELECT DISTINCT user_id
                    FROM activity 
                    WHERE transportation_mode LIKE 'taxi'
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_5(self):
        """ Find all types of transportation modes and count how many activities that are
            tagged with these transportation mode labels. Do not count the rows where the
            mode is null. """

        query = """
                    SELECT COUNT(DISTINCT transportation_mode)
                    FROM activity
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

        query = """
                    SELECT COUNT(id), transportation_mode
                    FROM activity
                    WHERE transportation_mode != 'NULL'
                    GROUP BY transportation_mode
                    ORDER BY COUNT(id) DESC
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_6a(self):
        """ Find the year with the most recorded activities. """

        query = """
                    SELECT YEAR(start_date_time) AS year, COUNT(id) AS activities
                    FROM activity
                    GROUP BY year
                    ORDER BY activities DESC
                    LIMIT 1
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_6b(self):
        """ Is the year with the most activities the year with the most recorded hours? """

        year_minute_table = """
                                SELECT YEAR(start_date_time) AS year,
                                TIMESTAMPDIFF(MINUTE, start_date_time, end_date_time) as minutes
                                FROM activity 
                            """

        query = f"""
                    SELECT year, SUM(minutes) as total_minutes
                    FROM ({year_minute_table}) AS year_minute_table
                    GROUP BY year 
                    ORDER BY total_minutes DESC
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_7(self):
        """ Find the total distance (in km) walked in 2008, by user with id=112. """

        def calc_total_distance(row):
            sum_distance = 0
            for i in range(1, len(row)):
                distance = haversine((row[i - 1][1], row[i - 1][2]), (row[i][1], row[i][2]))
                sum_distance += distance
            return sum_distance

        query = """
                    SELECT activity.user_id, lat, lon
                    FROM activity INNER JOIN trackpoint
                    ON activity.id = trackpoint.activity_id
                    WHERE activity.user_id = 112 AND YEAR(date_time) = "2008" AND transportation_mode = "walk"
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        tot_distance = calc_total_distance(rows)
        print("Total distance:")
        print(tot_distance)

    def task_8(self):
        """ Find the top 20 users who have gained the most altitude meters. """

        tptable = """ SELECT * FROM trackpoint WHERE trackpoint.altitude <> -777 """

        query = f"""
                    SELECT user.id, activity.id, FLOOR(trackpoint.altitude*0.3048), date_time
                    FROM user 
                    INNER JOIN activity ON user.id=activity.user_id 
                    INNER JOIN ({tptable}) AS trackpoint ON activity.id=trackpoint.activity_id
                    ORDER BY user.id, activity.id, date_time
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        user_meters_gained = {}

        for i in range(0, len(rows) - 1):
            user_id, activity_id = rows[i][:2]
            if user_id not in user_meters_gained:
                user_meters_gained[user_id] = 0
            if activity_id == rows[i][1]:
                this_altitude = rows[i][2]
                next_altitude = rows[i + 1][2]
                if next_altitude > this_altitude:
                    user_meters_gained[user_id] += next_altitude - this_altitude

        top_20 = sorted(user_meters_gained.items(), key=lambda tup: tup[1])[-20:]

        top_20.reverse()

        print(tabulate(top_20, headers=("user_id", "meters_gained")))

    def task_9(self):
        """ Find all users who have invalid activities, and the number of invalid activities per user. """

        time_difference_table = """
                                SELECT activity_id, TIMESTAMPDIFF(MINUTE, @timestamp, date_time) AS minute_difference,
                                @timestamp:=date_time curr_timestamp
                                FROM trackpoint
                                ORDER BY activity_id, date_time
                              """

        invalid_activity_table = f"""
                                    SELECT DISTINCT activity_id, IF(@activity_id = activity_id, 1, 0)
                                    AS is_new_activity, @activity_id:=activity_id
                                    FROM ({time_difference_table}) AS time_difference_table
                                    WHERE minute_difference >= 300
                                  """

        query = f"""
                    SELECT user_id, COUNT(activity_id) AS invalid_activity_count
                    FROM activity INNER JOIN ({invalid_activity_table}) AS invalid_activity_table 
                    ON activity.id = invalid_activity_table.activity_id
                    WHERE is_new_activity = 0
                    GROUP BY user_id
                 """

        variable_setup = "SET @timestamp='2000-01-01 00:00:00', @activity_id=0;"

        self.cursor.execute(variable_setup)
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_10(self):
        """ Find the users who have tracked an activity in the Forbidden City of Beijing. """

        query = """
                    SELECT DISTINCT activity.user_id
                    FROM activity
                    INNER JOIN trackpoint ON activity.id = trackpoint.activity_id
                    WHERE ABS(lat - 39.916) < 0.001 AND ABS(lon - 116.397) < 0.001
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_11(self):
        """ Find all users who have registered transportation_mode and their most used transportation_mode. """

        query = """
                    SELECT user.id, transportation_mode, COUNT(transportation_mode) AS transportation_count
                    FROM user INNER JOIN activity ON user.id=activity.user_id
                    WHERE transportation_mode <> "NULL"
                    GROUP BY user.id, transportation_mode
                    HAVING transportation_count > 0
                    ORDER BY user.id, transportation_count DESC
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        user_top_transport_mode = {}

        for row in rows:
            if row[0] not in user_top_transport_mode:
                user_top_transport_mode[row[0]] = row[1]

        print(tabulate(user_top_transport_mode.items(), headers=("user_id", "most_used_transportation_mode")))


def main():
    program = Tasks()
    program.task_1()
    program.task_2()
    program.task_3()
    program.task_4()
    program.task_5()
    program.task_6a()
    program.task_6b()
    program.task_7()
    program.task_8()
    program.task_9()
    program.task_10()
    program.task_11()


if __name__ == '__main__':
    main()

