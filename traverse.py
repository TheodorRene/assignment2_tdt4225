import os
import sys


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
        root, dirs, _ = next(os.walk(self.ROOT, topdown=True))
        dirs.sort()
        path_to_users = []
        for name in dirs:
            path_to_users.append(os.path.join(root, name))
        return path_to_users

    def get_all_plt_by_user_id(self, user_id):
        """
        Returns path to all plts for a user
        """
        path = self.ROOT + "/" + user_id + "/Trajectory"
        root, _, files = next(os.walk(path, topdown=True))
        paths = []
        for name in files:
            paths.append(os.path.join(root, name))
        return paths

def main():
    """
    Main function
    """
    file_object = FileTraversal()
    print(file_object.path_to_users())
    print(file_object.get_all_plt_by_user_id("000"))

if __name__ == '__main__':
    main()
