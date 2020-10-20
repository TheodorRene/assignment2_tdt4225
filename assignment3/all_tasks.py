from sys import argv
from DbConnector import DbConnector
from pprint import pprint

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
            pprint(collection_count)

        count_collection("user")
        count_collection("activity")
        count_collection("trackpoint")

    def task_5(self):
        """ Find all types of transportation modes and count how many activities that are
            tagged with these transportation mode labels. Do not count the rows where the
            mode is null. """

        collection = self.db['activity']
        distinct_transport = collection.distinct('transportation_mode')
        print("Amount of distinct transport labels: ",len(distinct_transport))

        pipeline = [
            {"$match": {'transportation_mode':{"$exists":"true"}}},
            {"$group":{'_id': '$transportation_mode',
                       'count':{'$sum':1}
                       }}
        ]
        pprint(list(collection.aggregate(pipeline)))


def main():
    program = Tasks()
    program.task_1()
    program.task_5()

if __name__ == '__main__':
    main()
