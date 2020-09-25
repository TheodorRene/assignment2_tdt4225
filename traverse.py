import os
import sys
from pathlib import Path


class FileTraversal:
    """
    Helper class for getting the path to the file you want <3
    """

    def __init__(self, ROOT="./dataset/dataset/Data"):
        self.ROOT = ROOT

    def path_to_users(self):
        """
        returns the path to all the users
        """
        p = Path(self.ROOT)
        return [x for x in p.iterdir() if x.is_dir()]

    def get_all_plt_by_user_id(self, user_id):
        """
        Returns path to all plts for a user
        """
        p = Path(self.ROOT) / user_id / "Trajectory"
        return [x for x in p.iterdir()]

    def get_all_ids(self):
        """
        returns the path to all the users
        """
        p = Path(self.ROOT)
        paths = [x for x in p.iterdir() if x.is_dir()]
        return sorted([x.name for x in paths])

    def has_labels(self, user_id):
        """ returns true if the user_id has labels """
        p = Path(self.ROOT) / user_id
        return len([x for x in p.iterdir()]) == 2


def main():
    """
    Main function
    """
    file_object = FileTraversal()
    #print(file_object.path_to_users())
    #print(file_object.get_all_plt_by_user_id("000"))
    all_ids = file_object.get_all_ids()
    for id in all_ids:
        if file_object.has_labels(id):
            print(id, True)

if __name__ == '__main__':
    main()
