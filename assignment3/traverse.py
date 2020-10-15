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

    def get_start_time_and_label(self, user_id):
        """ returns tuple of start_time and corrensponding label"""
        if not self.has_labels(user_id):
            return [()]
        labels_txt = Path(self.ROOT) / user_id / 'labels.txt'
        start_time_and_label = []
        with labels_txt.open() as f:
            for line in f.readlines()[1:]:
                start_date = self.parse_date_time(line.split("\t")[0])
                end_date = self.parse_date_time(line.split("\t")[1])
                label = line.split("\t")[-1].rstrip()
                start_time_and_label.append((start_date, label, end_date))
        return start_time_and_label

    def parse_date_time_line(self, timestamp_line):
        """ Takes a line from an .plt file and returns the timestamp"""
        return timestamp_line.split(
            ',')[-2] + " " + timestamp_line.split(",")[-1]

    def parse_date_time(self, timestamp):
        """ takes in timestamp from labels.txt and converts to DB format """
        return timestamp.split(' ')[0].replace(
            '/', '-') + " " + timestamp.split(" ")[1]

    def datetime_to_filename(self, timestamp):
        """ this will probably not be used, but converts DB timestamp to filename"""
        return timestamp.replace(
            " ",
            "").replace(
            "-",
            "").replace(
            ":",
            "") + ".plt"

    def filename_to_timestamp(self, filename):
        with filename.open() as f:
            lines = f.readlines()
            return self.parse_date_time_line(lines[6])


def main():
    """
    Main function
    """
    file_object = FileTraversal()
    # print(file_object.path_to_users())
    # print(file_object.get_all_plt_by_user_id("000"))
    s = file_object.filename_to_timestamp("20090611221204.plt")
    print(s)


if __name__ == '__main__':
    main()
