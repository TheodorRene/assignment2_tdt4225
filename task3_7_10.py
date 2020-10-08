from sys import argv
from tabulate import tabulate
from DbConnector import DbConnector
from haversine import haversine


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

    def task3(self):
        # Find the top 20 users with the highest number of activities.
        query = 'SELECT user_id, COUNT(user_id) num_of_activities' \
                '   from activity ' \
                '   GROUP BY(user_id)' \
                '   ORDER BY num_of_activities DESC' \
                '   LIMIT 20'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task7(self):
        # Find the total distance (in km) walked in 2008, by user with id=112.
        def calc_total_distance(row):
            sum_distance = 0
            for i in range(1, len(row)):
                distance = haversine((row[i - 1][1], row[i - 1][2]), (row[i][1], row[i][2]))
                sum_distance += distance
            return sum_distance

        query = 'SELECT activity.user_id, lat, lon' \
                '    FROM activity' \
                '    INNER JOIN trackpoint ON activity.id = trackpoint.activity_id' \
                '    WHERE activity.user_id = 112 AND YEAR(date_time) = "2008" AND transportation_mode = "walk"'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        tot_distance = calc_total_distance(rows)
        print("Total distance:")
        print(tot_distance)

    def task10(self):
        # Find the users who have tracked an activity in the Forbidden City of Beijing.
        query = 'SELECT DISTINCT activity.user_id' \
                '   FROM activity' \
                '   INNER JOIN trackpoint ON activity.id = trackpoint.activity_id' \
                '   WHERE ABS(lat - 39.916) < 0.001 AND ABS(lon - 116.397) < 0.001'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = Tasks()
    # program.task3()
    #program.task7()
    program.task10()


if __name__ == '__main__':
    main()
