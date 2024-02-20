#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###########################################################################
# Copyright (C) Dell EMC 2020-2022
# All rights reserved.
# Licensed material -- property of Dell EMC
#
# humanize_filesystem.py
#
###########################################################################
"""
When the user executes stitch.py it processes all metrics files which are obtained by un-tarring of the archive file.
These files contain alphanumeric values for metrics objects and long integer values which are not in human readable
format.

This program processes the "processed-metrics" directory obtained after executing stitch.py and adds additional columns
in performance object files to do a lookup for user friendly object names. It also adds columns to equate user friendly
rates converting bytes per second to megabytes per second. This program helps in converting bytes to kilobytes.
Configuration files are used to add user friendly object names.

"""""
from __future__ import print_function

from humanize_base import Humanize, get_full_path_for_file


class HumanizeFileSystem(Humanize):
    # This is the name of the file which has file system details.
    FILE_SYSTEM_FILENAME = 'file_system.csv'

    lookup_dict = dict()

    def __init__(self, input_directory=None):
        super(HumanizeFileSystem, self).__init__(input_directory)

        file_system_csv_files = self.create_file_list(input_directory, self.FILE_SYSTEM_FILENAME)
        self.lookup_dict = self.perform_lookup(file_system_csv_files)

    def add_details(self, filename):
        """
        Adds file system name in performance metrics for file system file by performing a lookup against
        file_system_id.  A list is created with all the entries in the file. File system name is appended
        at the end of each entry depending upon the corresponding file system id. The list is then written
        in the same file with the help of CSV writer.

        Args:
            filename (str): Name of metrics file that is to update.

        Returns:
            records_written: Records written in metric file system file.

        Test:
            test_humanize_base.py::test_add_file_system_name.
        """""
        if not self.lookup_dict:
            print('No records available for file system lookup')
            return 0

        input_filename = get_full_path_for_file(self.PROCESSED_METRICS_OUTPUT_DIR, filename)

        try:
            # Create a list of all contents in the file.
            file_data = self.read_file_data(input_filename)

        except IOError:
            # Raise ValueError if file is not found.
            print("Could not process file {}".format(filename))
            return 0

        # File system name is added against each file system id using the lookup.
        file_data_with_name = self.perform_lookup_with_id(file_data, self.lookup_dict, 'file_system_name',
                                                          'file_system_id')

        # Once the data is modified by appending name in the end it is written in the same file.
        records_written = self.write_to_file(filename, file_data_with_name)

        print('Records written in {} are {}'.format(filename, records_written))

        return records_written
