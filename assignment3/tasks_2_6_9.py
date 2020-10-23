import datetime
import time
from pprint import pprint
from DbConnector import DbConnector
from sys import argv


class Program:
    def __init__(self):
        self.connection = DbConnector(
            HOST='tdt4225-19.idi.ntnu.no',
            DATABASE='my_db',
            USER='admin_mydb',
            PASSWORD=argv[1]
        )
        self.client = self.connection.client
        self.db = self.connection.db

    def task_2(self):
        """ Find the average activities per user. """

        pprint(self.db["activity"].count() / self.db["user"].count())

    def task_6a(self):
        """ Find the year with the most recorded activities. """

        collection = self.db["activity"]
        documents = collection.aggregate([
            {
                "$group": {
                    "_id": {
                        "year": {"$year": {"$toDate": "$start_date_time"}}
                    },
                    "activity_count": {"$sum": 1}
                }
            },
            {
                "$sort": {"activity_count": -1}
            },
            {
                "$limit": 1
            }
        ]
        )
        for document in documents:
            pprint(document)

    def task_6b(self):
        """ Is the year with the most activities the year with the most recorded hours? """

        collection = self.db["activity"]
        documents = collection.aggregate([
            {
                "$group": {
                    "_id": {
                        "year": {"$year": {"$toDate": "$start_date_time"}}
                    },
                    "minute_count": {
                        "$sum": {
                            "$divide": [
                                {
                                    "$subtract":
                                        [
                                            {"$toDate": "$end_date_time"},
                                            {"$toDate": "$start_date_time"}
                                        ]
                                },
                                60000
                            ]
                        }
                    }
                }
            },
            {
                "$sort": {"minute_count": -1}
            },
            {
                "$limit": 1
            }
        ]
        )
        for document in documents:
            pprint(document)

    def task_9(self):
        """ Find all users who have invalid activities, and the number of invalid activities per user. """

        def _minute_difference(from_date, to_date):
            """ Calculates the difference in minutes between @param from_date and @param to_date. """
            from_date_stamp = time.strptime(from_date, "%Y-%m-%d %H:%M:%S")
            to_date_stamp = time.strptime(to_date, "%Y-%m-%d %H:%M:%S")

            from_date_unix = time.mktime(from_date_stamp)
            to_date_unix = time.mktime(to_date_stamp)

            return int((to_date_unix - from_date_unix) / 60)

        def _find_user_by_activity_id(db, activity_id):
            """ Returns the user that the given @param activity_id belongs to. """

            activities = db["activity"]
            activity = activities.find({"_id": activity_id})
            for activit in activity:
                return activit["user_id"]

        collection = self.db["trackpoint"]
        documents = collection.aggregate(
            [
                {"$project": {"_id": 1, "date_time": 1, "activity_id": 1}},
                {"$sort": {"activity_id": 1, "date_time": 1}},
            ], allowDiskUse=True
        )

        invalid_activities = []
        invalid_activity_count = {}
        previous_document = None
        for document in documents:
            if not previous_document or (previous_document["activity_id"] != document["activity_id"]):
                previous_document = document
                continue

            if _minute_difference(previous_document["date_time"], document["date_time"]) >= 5:
                if document["activity_id"] not in invalid_activities:
                    invalid_activities.append(document["activity_id"])

            previous_document = document

        print(len(invalid_activities))
        for invalid_activity in invalid_activities:
            invalid_user = _find_user_by_activity_id(self.db, invalid_activity)

            if invalid_user in invalid_activity_count.keys():
                invalid_activity_count[invalid_user] += 1
            else:
                invalid_activity_count[invalid_user] = 1

        pprint(invalid_activity_count)


def main():
    try:
        program = Program()
        program.task_2()
        program.task_6a()
        program.task_6b()
        program.task_9()

    except Exception as e:
        print("ERROR: ", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
