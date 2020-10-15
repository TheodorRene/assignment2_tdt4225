"""
File for assignmetn
"""
from sys import argv
from tabulate import tabulate
from DbConnector import DbConnector
from traverse import FileTraversal
from pprint import pprint
import uuid
import time
from pymongo import UpdateOne


class Assignment:
    """
    Main class
    """

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
        self.fs_helper = FileTraversal()

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)

    def insert_users(self):
        """ Adds all the users into the database """
        print("Adding users")
        users_ids = self.fs_helper.get_all_ids()
        docs = []
        for user_id in users_ids:
            has_label = self.fs_helper.has_labels(user_id)
            doc = {
                "_id": str(user_id),
                "has_labels": has_label,
            }
            docs.append(doc)
        collection = self.db['user']
        collection.insert_many(docs, ordered=False)


    def insert_activity_for_user(self, user_id):
        """ Add activity for a single user """
        plts = self.fs_helper.get_all_plt_by_user_id(user_id)
        docs = []
        for plt in plts:
            with plt.open() as f:
                lines = f.readlines()
                length_of_file = len(lines)
                if length_of_file <= 2500:
                    start_date_time = self.fs_helper.parse_date_time_line(
                        lines[6])
                    end_date_time = self.fs_helper.parse_date_time_line(
                        lines[length_of_file - 1])
                    doc = {
                        "_id":str(uuid.uuid4()),
                        "start_date_time": start_date_time.rstrip(),
                        "end_date_time": end_date_time.rstrip(),
                        "user_id": user_id
                    }
                    docs.append(doc)
        if docs:
            collection = self.db['activity']
            collection.insert_many(docs, ordered=False)
        else:
            print("user_id", user_id, "does not have any activites or all activites are longer than 2500 lines")

    def add_transportation_mode(self, user_id):
        """Add transportation_mode to existing activites for one user """
        data = self.fs_helper.get_start_time_and_label(user_id)
        if data == [()]: # user does not have labels
            return

        print("Adding transportation_mode for user", user_id)
        operations = []
        for records in data:
            myquery = {'user_id':user_id,"start_date_time":records[0], "end_date_time":records[2]}
            newvalues = {"$set": {"transportation_mode": records[1]}}
            operations.append(UpdateOne(myquery, newvalues))
        collection = self.db['activity']

        print(len(operations), " number of operations will be done")
        collection.bulk_write(operations)


    def add_transportation_mode_all(self):
        """ Add transportation_mode to all users """
        users_ids = self.fs_helper.get_all_ids()
        for user_id in users_ids:
            print("\x1b[2J\x1b[H INSERTING TRANSPORTATION MODE", round(((int(user_id)+1)/182) * 100, 2), "%")
            self.add_transportation_mode(user_id)

    def insert_activities(self):
        """ Add activity for all users"""
        print("adding activites")
        users_ids = self.fs_helper.get_all_ids()
        for user_id in users_ids:
            print("\x1b[2J\x1b[H INSERTING ACTIVITIES", round(((int(user_id)+1)/182) * 100, 2), "%")
            self.insert_activity_for_user(user_id)

    def insert_activities_with_label(self):
        """ Add activity and transportation_mode for all users"""
        self.insert_activities()
        self.add_transportation_mode_all()

    def insert_trackpoints(self):
        """ Add trackpoints for all users"""
        users_ids = self.fs_helper.get_all_ids()
        for user_id in users_ids:
            self.insert_trackpoint_by_user_id(user_id)

    def insert_trackpoint_by_user_id(self, user_id):
        """ Add trackpoints for a user """
        print("\x1b[2J\x1b[H INSERTING TRACKPOINTS", round(((int(user_id)+1)/182) * 100, 2), "%")
        plts = self.fs_helper.get_all_plt_by_user_id(user_id)
        for plt in plts:
            date_id = self.fs_helper.filename_to_timestamp(plt).rstrip()
            with plt.open() as f:
                lines = f.readlines()
                length_of_file = len(lines)
                if length_of_file <= 2500:
                    activity_id = self.get_activity_id_by_date(
                        user_id, date_id)
                    docs = []
                    for line in lines[6:]:
                        lat, lon, _, altitude, date_days, date, time_ = line.split(
                            ',')
                        date_time = date + " " + time_.rstrip()
                        doc = {
                            "activity_id": activity_id,
                            "lat": lat,
                            "lon": lon,
                            "altitude": altitude,
                            "date_days": date_days,
                            "date_time": date_time
                        }
                        docs.append(doc)
                    collection = self.db['trackpoint']
                    print("Inserting", len(docs), "documents")
                    collection.insert_many(docs, ordered=False)

    def get_activity_id_by_date(self, user_id, date_id):
        """ Get activity_id by date and user_id """
        collection = self.db['activity']
        query = {
            'user_id':str(user_id),
            "start_date_time": str(date_id)
        }
        dic = collection.find_one(query)
        if dic:
            return dic['_id']
        print("No match for user_id:", user_id, "and date_id", date_id)

def main():
    """
    Main function
    """
    program = None
    try:
        program = Assignment()
        #program.create_coll('user')
        #program.create_coll('activity')
        #program.create_coll('trackpoint')
        program.drop_coll('user')
        program.drop_coll('activity')
        program.drop_coll('trackpoint')
        program.insert_users()
        program.insert_activities_with_label()
        program.insert_trackpoints()
        program.fetch_documents('user')
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
