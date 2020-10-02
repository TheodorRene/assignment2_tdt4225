from tabulate import tabulate

class Tasks:

    
 def task_4(self):
        query = """ SELECT DISTINCT user_id
                    FROM activity 
                    WHERE transportation_mode LIKE 'taxi'
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))



def task_8(self):
        query = """ SELECT user.id, activity.id, trackpoint.altitude, date_time
                    FROM user INNER JOIN activity ON user.id=activity.user_id INNER JOIN trackpoin ON activity.id=trackpoint.activity_id
                    WHERE trackpoint.altitude <> -777
                    ORDER BY user.id, activity.id, date_time
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        user_meters_gained = {}

        for i in range(0, len(rows)-1):
            user_id, activity_id = rows[i][:2]
            if user_id not in user_meters_gained:
                user_meters_gained[user_id] = 0
            if activity_id == rows[i][1]:
                this_altitude = rows[i][2]
                next_altitude = rows[i+1][2]
                if next_altitude > this_altitude:
                    user_meters_gained[user_id] += next_altitude - this_altitude

        top_20 = sorted(user_meters_gained.items(), key=lambda tup: tup[1])[-20:]

        top_20.reverse()

        print(tabulate(top_20, headers=("user_id", "meters_gained")))



def task_11(self):
        query = """ SELECT user.id, transportation_mode, count(transportation_mode) AS transportation_count
                    FROM user INNER JOIN activity ON user.id=activity.user_id
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
