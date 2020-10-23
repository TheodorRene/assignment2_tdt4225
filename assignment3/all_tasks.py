import math
import time
from sys import argv
from DbConnector import DbConnector
from pprint import pprint
from haversine import haversine


class Tasks:
    def __init__(self):
        """
        Passord er sendt på facebook
        """
        self.connection = DbConnector(
            HOST='tdt4225-19.idi.ntnu.no',
            DATABASE='my_db',
            USER='admin_mydb',
            PASSWORD=argv[1]
        )
        self.db = self.connection.db
        self.client = self.connection.client

    def task_1(self):
        """ How many users, activities and trackpoints are there in the dataset? """

        def count_collection(collection_name):
            collection_count = self.db[collection_name].count_documents({})
            print(collection_name + " " + str(collection_count))

        count_collection("user")
        count_collection("activity")
        count_collection("trackpoint")

    def task_2(self):
        """ Find the average activities per user. """

        pprint(self.db["activity"].count() / self.db["user"].count())

    def task_3(self):
        """ Find the top 20 users with the highest number of activities."""
        activity_collection = self.db['activity']

        query = [
            {'$sortByCount': '$user_id'},
            {'$limit': 20}
        ]
        result = activity_collection.aggregate(query)

        for r in result:
            pprint(r)

    def task_4(self):
        """Find all users who have taken a taxi"""
        collection = self.db["activity"]
        documents = collection.find(
            {"transportation_mode": "taxi"}, {"user_id": 1}).distinct("user_id")

        for doc in documents:
            pprint(doc)

    def task_5(self):
        """ Find all types of transportation modes and count how many activities that are
            tagged with these transportation mode labels. Do not count the rows where the
            mode is null. """

        collection = self.db['activity']
        distinct_transport = collection.distinct('transportation_mode')
        print("Amount of distinct transport labels: ", len(distinct_transport))

        pipeline = [
            {"$match": {'transportation_mode': {"$exists": "true"}}},
            {"$group": {'_id': '$transportation_mode',
                        'count': {'$sum': 1}
                        }}
        ]
        pprint(list(collection.aggregate(pipeline)))

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

    def task_7(self):
        """Find the total distance (in km) walked in 2008, by user with id=112."""

        def calc_total_distance(row):
            sum_distance = 0
            for r in row:
                for i in range(len(r['all_trackpoints'])):
                    distance = haversine(
                        (float(r['all_trackpoints'][i - 1]['lat']), float(r['all_trackpoints'][i - 1]['lon'])),
                        (float(r['all_trackpoints'][i]['lat']), float(r['all_trackpoints'][i]['lon'])))
                    sum_distance += distance
            return sum_distance

        activity_collection = self.db['activity']

        query = [
            {'$match':
                {'user_id': '112',
                 'transportation_mode': 'walk',
                 '$expr': {'$eq': [{'$year': {'$toDate': '$start_date_time'}}, 2008]}
                 }
             },
            {'$lookup':
                {'from': 'trackpoint',
                 'localField': '_id',
                 'foreignField': 'activity_id',
                 'as': 'all_trackpoints'
                 }
             },
            {'$project': {'_id': 1, 'lat': 1, 'lon': 1, 'all_trackpoints': 1}},
            {'$sort': {'_id': 1}}
        ]

        result = activity_collection.aggregate(query)
        tot_distance = calc_total_distance(result)
        print("Total distance:")
        pprint(tot_distance)

    def task_8(self):
        """Find the top 20 users who have gained the most altitude meters"""
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
            elif (float(point['altitude']) > float(previous_point['altitude'])) and point['activity_id'] == \
                    previous_point['activity_id']:
                user_metres_dict[activity['user_id']
                ] += math.floor((float(point['altitude']) - float(previous_point['altitude'])) * 0.3048)
            previous_point = point

        top_20 = sorted(user_metres_dict.items(), key=lambda tup: tup[1])[-20:]
        top_20.reverse()
        print("Top 20:")
        pprint(top_20)

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
            activity = activities.find_one({"_id": activity_id})
            return activity["user_id"]

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

        for invalid_activity in invalid_activities:
            invalid_user = _find_user_by_activity_id(self.db, invalid_activity)

            if invalid_user in invalid_activity_count.keys():
                invalid_activity_count[invalid_user] += 1
            else:
                invalid_activity_count[invalid_user] = 1

        pprint(invalid_activity_count)

    def task_10(self):
        """Find the users who have tracked an activity in the Forbidden City of Beijing."""

        def find_users(data):
            users = set()
            for r in data:
                users.add(r['user'][0]['user_id'])
            return users

        trackpoint_collection = self.db['trackpoint']

        query = [
            {'$match':
                {'$expr': {'$and': [{'$and': [{'$gte': ['$lat', '39.915']}, {'$lte': ['$lat', '39.917']}]},
                                    {'$and': [{'$gte': ['$lon', '116.396']}, {'$lte': ['$lon', '116.398']}]}]}}
             },
            {'$lookup':
                {'from': 'activity',
                 'localField': 'activity_id',
                 'foreignField': '_id',
                 'as': 'user'
                 }
             },
            {'$project': {'user': 1}}
        ]

        result = trackpoint_collection.aggregate(query)
        all_users = find_users(result)
        print("Has been in the Forbidden City of Beijing:")
        pprint(all_users)

    def task_11(self):
        """Find all users who have registered transportation_mode and their most used transportation_mode."""
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
