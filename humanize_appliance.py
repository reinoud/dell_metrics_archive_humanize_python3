#!/usr/bin/env python
# -*- coding: utf-8 -*-
###########################################################################
# Copyright (C) Dell EMC 2020-2021
# All rights reserved.
# Licensed material -- property of Dell EMC
#
# humanize_appliance.py
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

from humanize_base import Humanize, get_full_path_for_file, COLUMN_HEADER_APPLIANCE_ID


# Headers for appliance metrics file.
SERIAL_NUMBER = 'serial_number'
TYPE = 'type'
MODEL = 'model'
SERVICE_TAG = 'service_tag'
MODE = 'mode'

# Name of file which has appliance details.
APPLIANCE_FILENAME = 'appliance.csv'


class HumanizeAppliance(Humanize):

    lookup_dict = dict()

    def __init__(self, input_directory=None):
        super(HumanizeAppliance, self).__init__(input_directory)

        # From the input directories, get list of all appliance CSV files.
        appliance_csv_files = self.create_file_list(input_directory, APPLIANCE_FILENAME)

        # Create dictionary with key as appliance id and value as a dictionary of values mentioned in the list
        self.lookup_dict = self.perform_nested_lookup(appliance_csv_files, [SERIAL_NUMBER, TYPE, MODEL, SERVICE_TAG,
                                                                            MODE])

    def add_details(self, filename):
        """
         A list is created with all the entries in the file. Adds additional column in appliance metrics file. This
         method adds details such as service number model, type, model and service tag. These details are added using
         appliance id from the appliance lookup. Once values are appended the file is written in the output directory.

        Args:
            filename (str): Name of metrics file that is to update.

        Returns:
            Records written in metrics file.

        Exception:
            ValueError if file to update does not exist.
        """""
        if not self.lookup_dict:
            print('No records available for appliance lookup')
            return 0

        input_filename = get_full_path_for_file(self.PROCESSED_METRICS_OUTPUT_DIR, filename)

        # Read data from appliance metrics file
        try:
            appliance_file_data = self.read_file_data(input_filename)

        except IOError:
            # Return if file could not be processed.
            print("Could not process file {}".format(filename))
            return 0

        # Append appliance details by performing a lookup.
        data_with_appliance_details = self.add_data_from_lookup(appliance_file_data, self.lookup_dict,
                                                                [SERIAL_NUMBER, TYPE, MODEL, SERVICE_TAG, MODE])

        # After appending appliance details write to file in output directory keeping same file name.
        appliance_records = self.write_to_file(filename, data_with_appliance_details)

        print('Records written in {} are {}'.format(filename, appliance_records))

        return appliance_records

    @staticmethod
    def add_data_from_lookup(file_data, config_lookup, header_list):
        """
        Adds details from the lookup using id. If these details are not found append id.

        Args:
            file_data (list[]): File data from appliance metrics file.
            config_lookup (dict{{}}): Dictionary with key as id and value as a dictionary of config details.
            header_list(list[]): List of header names to append.

        Returns:
            file data after lookup values are added.

        Test:
            test_humanize_base.py::test_add_data_from_lookup
        """""
        # Header for object name is appended in the headers row.
        file_data[0].extend(header_list)

        # Get appliance id index.
        appliance_id_index = file_data[0].index(COLUMN_HEADER_APPLIANCE_ID)

        # Lookup for name from id_name_lookup for all rows excluding the header.
        for data in file_data[1:]:
            # Get values using  id as the key. If key does not exist return an empty list as default.
            details_list = config_lookup.get(data[appliance_id_index], {}).values()

            # If values exits append it to the file data else append appliance id
            data.extend(details_list if details_list else [data[appliance_id_index]])

        return file_data
