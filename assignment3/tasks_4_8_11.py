from pprint import pprint
from DbConnector import DbConnector
from sys import argv
import math


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

    def task_4(self):
        collection = self.db["activity"]
        documents = collection.find(
            {"transportation_mode": "taxi"}, {"_id": 1})

        for doc in documents:
            pprint(doc)

    def task_8(self):
        valid_points = self.db["trackpoint"].aggregate([
            {"$match": {"altitude": {"$ne": "-777"}}},
            {"$lookup": {
                "from": "activity",
                "localField": "activity_id",
                "foreignField": "_id",
                "as": "activity"
            }}
        ])

        user_metres_dict = {}
        previous_point = None

        for point in valid_points:
            point = dict(point)
            activity = dict(point['activity'][0])

            if activity['user_id'] not in user_metres_dict.keys():
                user_metres_dict[activity['user_id']] = 0

            if previous_point == None:
                previous_point = point
            elif (float(point['altitude']) > float(previous_point['altitude'])) and point['activity_id'] == previous_point['activity_id']:
                user_metres_dict[activity['user_id']
                                 ] += math.floor((float(point['altitude']) - float(previous_point['altitude'])) * 0.3048)
            previous_point = point

        top_20 = sorted(user_metres_dict.items(), key=lambda tup: tup[1])[-20:]
        top_20.reverse()
        print("Top 20:")
        pprint(top_20)

    def task_11(self):
        collection = self.db["user"]
        documents = collection.aggregate([
            {"$match": {"has_labels": True}},
            {"$lookup": {
                "from": "activity",
                "localField": "_id",
                "foreignField": "user_id",
                "as": "activities"
            }}
        ])

        user_top_transport_mode = {}

        for user in documents:
            activities = user['activities']
            user_activities_count = {}

            for activity in activities:
                activity_dict = dict(activity)
                transport_mode = activity_dict.get("transportation_mode")
                if transport_mode != None:
                    if transport_mode in user_activities_count.keys():
                        user_activities_count[transport_mode] += 1
                    else:
                        user_activities_count[transport_mode] = 1

            if len(user_activities_count) > 0:
                top_activity = sorted(
                    user_activities_count.items(), key=lambda item: item[1])[-1][0]

            user_top_transport_mode[user['_id']] = top_activity

        pprint(user_top_transport_mode)


def main():
    program = None
    try:
        program = Program()
        # program.task_4()
        program.task_8()
        # program.task_11()

    except Exception as e:
        print("ERROR: ", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
