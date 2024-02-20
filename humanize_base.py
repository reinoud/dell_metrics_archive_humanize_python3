#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###########################################################################
# Copyright (C) Dell EMC 2020-2021
# All rights reserved.
# Licensed material -- property of Dell EMC
#
# humanize_base.py
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

import csv
import io
import os
import sys
from collections import namedtuple, OrderedDict

# Bytes to Kib or MiB converter.
BYTES_CONVERTER = 1024.0

# Name of the output directory for humanize.
OUTPUT_DIR = 'output'

# Helps to determine the current sys version of python being used as script supports multiple python version.
PY_VERSION_MAJOR = sys.version_info.major

# File name when volume details are present when system in HCI mode.
VIRTUAL_VOLUME_FILENAME = 'virtual_volume.csv'

COLUMN_HEADER_APPLIANCE_ID = 'appliance_id'
COLUMN_HEADER_NAME = 'name'


class Humanize(object):
    # Name of the output directory for stitch.py.
    PROCESSED_METRICS_OUTPUT_DIR = 'processed-metrics'

    def __init__(self, input_directory):
        pass

    @staticmethod
    def read_file_data(filename):
        """
        Csv file is read with the help of a CSV reader. A list containing all data in CSV is returned for
        further processing.

        Args:
            filename (str): Name of file whose data is converted to list and returned.

        Returns:
            A list of records in metric file.
        """""
        try:
            with open(filename) as file_reader_handle:
                file_reader = csv.reader(file_reader_handle)

                # Store the contents of the file as a list
                file_data = list(file_reader)
                return file_data

        except IOError:
            raise

    def write_to_file(self, table_name, file_data):
        """
        Write modified data to the specified file using CSV writer.

        Args:
            table_name (str): Name of the table that needs to be written.
            file_data (list[]): Data that is modified with the help of additional information.

        Returns:
             Count of the records written to the file_name.

        Exception:
            IOError if unable to write to file.
        """""
        records_written = 0
        open_mode, kwargs = self.get_mode_and_newline()
        file_name = get_full_path_for_file(OUTPUT_DIR, table_name)

        try:
            with io.open(file_name, open_mode, **kwargs) as csv_file:
                writer = csv.writer(csv_file, delimiter=',')

                for line in file_data:
                    records_written += 1
                    writer.writerow(line)

            return records_written

        except IOError:
            raise IOError("Additional Information cannot be written in {}".format(file_name))

    @staticmethod
    def get_mode_and_newline():
        """
        Get open mode and keywords for opening a file to write in Python version independent way.

        Returns:
            open mode and keywords to use when you are opening a file to write.t
        """""
        # Adjust open arguments for Python2 vs 3
        if PY_VERSION_MAJOR > 2:
            # For new version of Python.
            open_mode = 'wt'
            kwargs = {'newline': ''}
        else:
            # For old version of Python.
            open_mode = 'wb'
            kwargs = {}

        return open_mode, kwargs

    def create_file_list(self, input_dirs, *filename_list):
        """
        Create a list of all files present in the input directory for which a lookup needs to be performed.

        Args:
            input_dirs ([str]): The directory which has all subdirectories that include performance and
                                config CSV files.
            filename_list([str]): Create a list of filenames from all the remaining parameters presented.

        Returns:
            A list of CSV file names.
        """""
        csv_filenames = []

        for directory in input_dirs:
            # Process the input directory
            directory_name = os.path.abspath(directory)

            # Process every file in the file list for all directories in the input directories.
            [csv_filenames.extend(self.get_files_list(directory_name, filename)) for filename in filename_list]

        return csv_filenames

    def get_files_list(self, directory_name, filename):
        """
        Returns a list of specified files from the directory.

        Args:
            directory_name (str): Current working directory. Caller can do this: directory_name = getcwd()
            filename (str): Name of the file whose file path list is created.

        Returns:
            A list of specified CSV files.

        Test:
            test_humanize_base.py::test_get_files_list
        """""
        # This is the total list of CSV files that have been found in the subdirectories.
        csv_files_list = []

        # This loop creates a file path for every single CSV file, including the top level file names.
        for subdir, _, files in os.walk(directory_name):
            # Iterate through the subdirectories and get a list of CSV files in each directory.
            csv_files_list.extend([os.path.join(subdir, local_file) for local_file in files
                                   if local_file.lower() == filename and os.path.splitext(local_file)[1].lower() ==
                                   '.csv'])

        # From the list of fully qualified file names that are CSV files, filter out files that do not any data.
        csv_files_list_with_data = [full_file_name for full_file_name in csv_files_list
                                    if self.table_has_data(full_file_name)]

        return csv_files_list_with_data

    @staticmethod
    def table_has_data(filename):
        """
        Determines if there is at least one row of data apart from the header.

        Args:
            filename (str): Name of file to check if at least one record is present.

        Returns:
            Boolean value depending if the data exists in the filename.

        Exception:
            IOError if unable to open metric file.

        Test
            test_humanize::test_table_has_data
        """""
        try:
            with open(filename, 'r') as file_handle:
                reader = csv.reader(file_handle)
                try:
                    next(reader)

                except StopIteration:
                    # The file was empty. Swallow the exception and continue.
                    return False

                # Check for a row of data.
                try:
                    next(reader)

                    # A row of data exists to reach this code point.
                    return True

                except StopIteration:
                    # The file did not have a row of data. Swallow the exception and continue.
                    return False

        except IOError:
            raise ValueError('Could not open file {}'.format(filename))

    def perform_lookup(self, csv_file_list):
        """
        Create a dictionary with the key as id of the required metric system and value as the name of the corresponding
        metric system.

        Args:
            csv_file_list ([list]): List of CSV files which has id and name of the metrics system. Dictionary is
                                    created with the help of entries in these files.

        Returns:
            A dictionary where key is the id of metric system and value is the corresponding human readable name.

        Exceptions:
            ValueError if file is not accessible while creating dictionary for lookup.

        Test:
            test_humanise.py::test_create_lookup
        """""
        id_name_dict = dict()

        for filename in csv_file_list:
            try:
                with open(filename) as file_handle:
                    reader = csv.reader(file_handle)

                    # Declaring namedtuples to convert row of record in the file to
                    # namedtuple object for further processing
                    # file headers are used to declare namedtuples.
                    # Eg: Row = namedtuple('Row', ['first', 'second', 'third'])
                    data = namedtuple('FileData', next(reader))

                    for row in reader:
                        # Convert list to namedtuple.
                        # Eg: Row = namedtuple('Row', ['first', 'second', 'third'])
                        # A = ['1', '2', '3']
                        # Row(*A)
                        # Row(first='1', second='2', third='3')

                        file_data = data._make(row)
                        file_dict = file_data._asdict()

                        # Get name from vmw_volume column for virtual_volume.csv file else from name.
                        id_name_dict[file_dict.get('id')] = file_dict.get('vmw_vvolname') \
                            if VIRTUAL_VOLUME_FILENAME in filename \
                            else file_dict.get('name')

            except IOError:
                raise ValueError('Could not open file {}'.format(filename))

        return id_name_dict

    def perform_nested_lookup(self, csv_file_list, column_list):
        """
        Create a dictionary using appliance config file with key as the id of appliance and value as another dictionary
        consisting of appliance details. These details are appended in appliance metrics based on appliance id.

        Args:
            csv_file_list (list[]): List of CSV config files.
            column_list([]): List of nested keys to create for lookup.

        Returns:
            A nested dictionary with key as id and value with details present in column_list

        Exception:
            ValueError if file is not accessible while creating dictionary for lookup.

        Test:
            test_humanize_base.py::test_perform_nested_lookup
        """""
        value_lookup = OrderedDict()

        # Process every config file in the list
        for filename in csv_file_list:
            try:
                with open(filename) as file_handle:
                    reader = csv.reader(file_handle)
                    data = namedtuple('FileData', next(reader))

                    for row in reader:
                        file_data = data._make(row)

                        # Lookup to have id as the key and with nested dictionary as the value.
                        # Eg: value_lookup = {'A1': {'name': 'appliance-1', 'node': 'N1'},
                        #                     'A2': {'name': 'appliance-2', 'node': 'N2' } }
                        # Nested dictionary can have any configurable keys depending on the table it is being used for
                        # hence
                        # a column_list is passed as to create nested dictionary with keys as the required
                        # columns in file.
                        # '__getattribute__' returns the attribute value from the namedtuple object to use
                        # as a value for nested dictionary.
                        value_lookup[file_data.id] = OrderedDict(
                            list(map(lambda column: (
                                column,
                                file_data.__getattribute__(column) if file_data.__getattribute__(column) else 0),
                                     column_list)))

            except IOError:
                print ('Could not open file {}'.format(filename))
                return OrderedDict()

        return value_lookup

    def add_details(self, filename):
        pass

    @staticmethod
    def perform_lookup_with_id(file_data, id_name_lookup, column_header_name, column_name):
        """
        Performance object name is added for each entry in the file against the corresponding object id.

        Args:
            file_data (list[]): List of all entries in the metrics file which needs to be modified in order to add
                                metrics name using the lookup dictionary.
            id_name_lookup (dict): A dictionary with id (key) and the corresponding human readable metric name (value)
            column_header_name (str): Column header name for the metric name that needs to be added in the header row.
            column_name (str): Column name where lookup key exists from file data.

        Returns:
             A list where each record has a name corresponding to its id in the end.

        Test:
            test_humanize_base.py::test_perform_lookup_with_id
        """""
        # Header for object name is appended in the headers row.
        file_data[0].append(column_header_name)

        # Get index of column used to perform lookup.
        column_index = file_data[0].index(column_name)

        # Lookup for name from id_name_lookup for all rows excluding the header.
        for data in file_data[1:]:
            # If the corresponding id does not have a name mapped to it, append id.
            data.append(id_name_lookup.get(data[column_index], data[column_index]))

        return file_data

    @staticmethod
    def convert_to_kib(value, digits):
        """
        Convert the given value from bytes to kilobytes and rounds up the given value up to one digit.

        Args:
            value (str / float): Value to be converted to Kilobytes.
            digits (int): The number of decimals to use when rounding the result.

        Returns:
            Kb value rounded to the number required number of digits.

        Test:
            test_humanize_base.py::test_convert_to_kib
        """""
        try:
            return round(float(value) / BYTES_CONVERTER, digits)

        except (ValueError, TypeError):
            print("Converting to KBytes failed for {} {}".format(value, digits))
            return 0.0

    @staticmethod
    def convert_to_mib(value, digits):
        """
        Converts a given number to Megabytes and rounds up the given value up to three digits.

        Args:
            value (str): Value to be converted to Megabytes.
            digits (int): The number of decimals to use when rounding the result.

        Returns:
             MB value rounded to the required number of digits.

        Test:
            test_humanize_base.py::test_convert_to_mib
        """""
        try:
            return round(float(value) / BYTES_CONVERTER / BYTES_CONVERTER, digits)

        except (ValueError, TypeError):
            print("Converting to MBytes failed for {} {}".format(value, digits))
            return 0

    def calculate_tx_rx(self, fe_eth_file_data):
        """
        Converts tx and rx per second to MiBPS for every record in file. Appends header to header row.

        Args:
            fe_eth_file_data (list []): List of records in metrics file.

        """""
        file_data = namedtuple('FileData', fe_eth_file_data[0])

        for data in fe_eth_file_data[1:]:
            fe_eth_data = file_data._make(data)

            # Calculates tx ps in MB
            tx_value = self.convert_to_mib(fe_eth_data.bytes_tx_ps, 3)
            data.append(tx_value)

            # Calculates rx ps in MB
            rx_value = self.convert_to_mib(fe_eth_data.bytes_rx_ps, 3)
            data.append(rx_value)

        fe_eth_file_data[0].extend(['Mbytes_tx_ps', 'Mbytes_rx_ps'])

        return fe_eth_file_data

    @staticmethod
    def calculate_total_mb(prev, curr, cache):
        """
        Calculates total MB by converting total bytes to MegaBytes.
        If timestamp for both the records is same then return 0.
        Total MB is calculated only if two consecutive records have same cache type and same node,
        otherwise total MB is 0.

        Args:
            prev (list[]): Previous record used for comparison purposes.
            curr (list[]): Current Record to calculate total bytes.
            cache (namedtuple): Header row to create namedtuple object.

        Test:
            test_humanize_base.py::test_calculate_total_mb
        """""

        prev_cache = cache(*prev)
        curr_cache = cache(*curr)

        # Perform calculation only if two consecutive records have different timestamp, same cache type and same node.
        # Return 0 if otherwise.
        if prev_cache.timestamp != curr_cache.timestamp and prev_cache.cache_type == curr_cache.cache_type \
                and prev_cache.node_id == curr_cache.node_id:
            return float(curr_cache.total_bytes) / BYTES_CONVERTER / BYTES_CONVERTER
        return 0.0

    @staticmethod
    def calculate_dirty_pages_local_in_gb(prev, curr, cache):
        """
        Calculates dirty pages local GB by converting dirty pages local to GigaBytes. If timestamp for both the records
        matches then return 0. Dirty pages local GB is calculated only if two consecutive records have same cache type
         and same node, otherwise dirty pages local GB is 0.

        Args:
            prev (list[]): Previous record used for comparison purposes.
            curr (list[]): Current Record to calculate dirty pages local in GB.
            cache (namedtuple): Header row to create namedtuple object.

        Returns:
            dirty_page_local GB value.

        Test:
            test_humanize_base.py::test_calculate_dirty_pages_local_in_gb
        """""

        prev_cache = cache(*prev)
        curr_cache = cache(*curr)

        # Perform calculation only if two consecutive records have different timestamp, same cache type and same node.
        # Return 0 if otherwise.
        if prev_cache.timestamp != curr_cache.timestamp and prev_cache.cache_type == curr_cache.cache_type and \
                prev_cache.node_id == curr_cache.node_id:
            return round(float(curr_cache.dirty_pages_local) * 4 / BYTES_CONVERTER / BYTES_CONVERTER, 2)
        return 0.0

    @staticmethod
    def calculate_dirty_pages_peer_gb(prev, curr, cache):
        """
        Calculates dirty pages peer GB. If timestamp for both the records matches then return 0. Dirty pages peer GB is
        calculated only if two consecutive records have same cache type and same node, otherwise dirty pages peer per
        second
        is 0.

        Args:
            prev (list[]): Previous record used for comparison purposes.
            curr (list[]): Current Record to calculate dirty pages peer GB.
            cache (namedtuple): Header row to create namedtuple object.

        Returns:
            dirty_pages_peer_peer_GB value.
        """""

        prev_cache = cache(*prev)
        curr_cache = cache(*curr)

        # Perform calculation only if two consecutive records have different timestamp, same cache type and same node.
        # Return 0 if otherwise.
        if prev_cache.timestamp != curr_cache.timestamp and prev_cache.cache_type == curr_cache.cache_type and \
                prev_cache.node_id == curr_cache.node_id:
            return round(float(curr_cache.dirty_pages_peer) * 4 / BYTES_CONVERTER / BYTES_CONVERTER, 2)
        return 0.0

    @staticmethod
    def calculate_total_dirty_pages_mbps(prev, curr, cache):
        """
        Calculates total dirty pages MBPS using total dirty pages.
        If timestamp for both the records matches then return 0.
        Total dirty pages MBPS is calculated only if two consecutive records
        have same cache type and same node, otherwise total dirty pages MBPS is 0.

        Args:
            prev (list[]): Previous record used for comparison purposes.
            curr (list[]): Current Record to calculate dirty pages local in MB.
            cache (namedtuple): Header row to create namedtuple object.

        Returns:
            total_dirty_pages_mbps value.

        Test:
            test_humanize_base.py::test_calculate_total_dirty_pages_mbps
        """""
        prev_cache = cache(*prev)
        curr_cache = cache(*curr)

        # Perform calculation only if two consecutive records have different timestamp, same cache type and same node.
        # Return 0 if otherwise.
        if prev_cache.timestamp != curr_cache.timestamp and prev_cache.cache_type == curr_cache.cache_type and \
                prev_cache.node_id == curr_cache.node_id:
            return round(float(curr_cache.total_dirty_bytes) / BYTES_CONVERTER / BYTES_CONVERTER, 2)
        return 0.0

    @staticmethod
    def calculate_pages_being_held_gb(prev, curr, cache):
        """
        Calculates pages being held in GB. If timestamp for both the records matches then return 0.
        Pages being held GB is calculated only if two consecutive records have same cache type and same node,
        otherwise pages being held is 0.

        Args:
            prev (list[]): Previous record used for comparison purposes.
            curr (list[]): Current Record to calculate pages being held GB.
            cache (namedtuple): Header row to create namedtuple object.

        Returns:
            pages_being_held_GB value.
        """""

        prev_cache = cache(*prev)
        curr_cache = cache(*curr)

        # Perform calculation only if two consecutive records have different timestamp, same cache type and same node.
        # Return 0 if otherwise.
        if prev_cache.timestamp != curr_cache.timestamp and prev_cache.cache_type == curr_cache.cache_type and \
                prev_cache.node_id == curr_cache.node_id:
            return round(float(curr_cache.pages_being_held) * 4 / BYTES_CONVERTER / BYTES_CONVERTER / 20, 2)
        return 0

    @staticmethod
    def compare_and_calculate(prev, curr, cache, value):
        """
        Determines the delta between two consecutive records and divide by sample time (20 seconds) to get rates. If
        timestamp for both the records matches then return value is 0. Value is calculated only if two consecutive
        records have same cache type and same node, otherwise value is 0.

        Args:
            prev (list[]): Previous record used for comparison purposes.
            curr (list[]): Current Record to get rates.
            cache: Object to create namedtuple
            value (str): Value to calculate.

        Returns:
            Delta value after comparison.

        Test:
            test_humanize_base.py::test_compare_and_calculate.
        """""

        prev_cache = cache(*prev)
        curr_cache = cache(*curr)

        # Perform calculation only if two consecutive records have different timestamp, same cache type and same node.
        # Return 0 if otherwise.
        if prev_cache.timestamp != curr_cache.timestamp and prev_cache.cache_type == curr_cache.cache_type and \
                prev_cache.node_id == curr_cache.node_id:
            return round((float(curr_cache.__getattribute__(value)) -
                          float(prev_cache.__getattribute__(value))) / 20, 2)
        return 0


def get_value_from_key(key, lookup, required_key):
    """
    Return value from the nested dictionary for the mentioned required value.

    Args:
        key(str): Key of the value to look for.
        lookup(dict{}): Nested Dictionary
        required_key(str): Key from the nested dictionary whole value is to be returned.

    Returns:
        Value of the mentioned required key.
    """""
    try:
        value = lookup.get(key).get(required_key)

    # If attribute is not found inside the nested dictionary.
    except AttributeError:
        value = lookup.get(key)

    # If the key is not found return key as value.
    return value if value else key


def get_full_path_for_file(directory_name, file_name):
    """
    This method concatenates directory path and filename using a directory separator.

    Args:
        directory_name (str): Directory path.
        file_name (str): File name to concatenate with directory.
    """""
    return os.path.join(directory_name, file_name)
