#!/usr/bin/env python
# -*- coding: utf-8 -*-
###########################################################################
# Copyright (C) Dell EMC 2019-2021
# All rights reserved.
# Licensed material -- property of Dell EMC
#
# stitch.py
#
###########################################################################
"""
When the user untars an archive file, there is a script (this file), a catalog file which contains
the frequency that each metrics table is updated, and a set of directories, each directory
representing a collection.  In each collection is a set of CSV files, each file representing a table.
If the table has a timestamp field, it is a metrics table, otherwise it is a configuration file.

This program processes the collection directories for metrics files that can be rehydrated and
stitched together so that overlapping data is removed, with the result being a single CSV file that
spans all of the collections.

This program does not process files that do not have timestamps.

"""""
from __future__ import print_function

import argparse
import collections
import csv
import json
from datetime import datetime, timedelta
import operator
import os
import io
import sys
from io import open

STITCH_VERSION = '3.0.0.0'
PY_VERSION_MAJOR = sys.version_info.major
IS_WINDOWS = (os.name == 'nt')

TableStart = collections.namedtuple('TableStart', 'filename start_date_time')

# Command line parameters.
CATALOG_FILENAME = 'archive_properties_catalog.json'
OUTPUT_DIR = "processed-metrics"

# These are the names for column headers that the code searches for.  The column headers from the
# table headers must be converted to lower case before using the following names.
COLUMN_HEADER_TIMESTAMP_LOWER = 'timestamp'
COLUMN_HEADER_REPEAT_COUNT = 'repeat_count'
COLUMN_HEADER_DELETED = 'deleted'

TWENTY_SECONDS = 20
ONE_MINUTE_IN_SECS = 60
FIVE_MINUTES_IN_SECS = 5 * ONE_MINUTE_IN_SECS
ONE_HOUR_IN_SECS = 60 * ONE_MINUTE_IN_SECS
ONE_DAY_IN_SECS = 24 * ONE_HOUR_IN_SECS


def main(cmd_args):
    """ Main processing. This can be called with an array of arguments.

    Args:
        cmd_args ([str]): The command line arguments minus the program name.

    """""
    # Get the command line arguments and process them.
    args = parse_args(cmd_args)

    input_dir_list = args.input_dir if args.input_dir else None

    top_level_processing(OUTPUT_DIR, input_dir_list)


def parse_args(args):
    """ This function provides the definition for allowed command line arguments and returns any
        of the arguments provided.  The argparse library will handle error and boundary checking
        on the provided arguments, returning an appropriate error.

    Args:
        args ([str]): The command line arguments minus the program name.

    Test:
        rehydrate_test.py::test_parse_args
    """""
    parser = argparse.ArgumentParser(description='Read in a list of files and stitch them together',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i',
                        '--input-dir',
                        nargs='+',
                        default='',
                        help='The directory to scan recursively for csv files')

    return parser.parse_args(args)


def top_level_processing(output_directory_name,
                         input_directory_name_list=None,
                         catalog_filename=CATALOG_FILENAME):
    """ This is the top level call which searches the current directory for collection subdirectories.
        In these directories, it looks for csv files that have timestamps in them.  Note, each file
        represents an instance of a table. This method then rehydrates the files that were compressed
        using a repeat_count field, sort the resulting timestamped entries from oldest to newest, then
        stitch files together for the same table (removing overlapping timestamps) and writes the
        results to an output directory.

    Args:
        input_directory_name_list ([str]): The directory to scan recursively for csv files.
        output_directory_name (str): The name of the directory to create, where output files get written.
        catalog_filename (str): The filename of the catalog to use.

    Returns:
        0 if success, -1 if failure

    Test:
        rehydrate_test.py::test_top_level_processing_results
    """""
    print('{} version {}\nWriting results to output directory {}'.format(os.path.basename(__file__), STITCH_VERSION,
                                                                         output_directory_name))

    # Read the JSON catalog into a dictionary
    catalog_obj = Catalog(catalog_filename)

    # Make sure that the output directory either doesn't exist or if it does, it is empty.
    try:
        create_output_directory(output_directory_name)

    except OSError:
        sys.exit(-2)

    # Handle the default input directory list.
    if input_directory_name_list is None:
        input_directory_name_list = [os.getcwd()]

    # From the input directories, get a list of files that have timestamped data.
    timestamped_files = make_list_of_files_with_timestamped_data(input_directory_name_list)

    # Traverse the timestamped files and create a dictionary of [filename,CSV-list] for the next stage
    # of processing.
    #
    # The dictionary has entries keyed by a filename of format "table_name.csv" and value which is a
    # list of CSV objects that represent segments of the larger table to create.  Each list is generated
    # from a single capture, and multiple captures are combined to form the complete timeline for that
    # table across the collections.
    try:
        table_dict = make_csv_file_dict(timestamped_files, catalog_obj)
    except ValueError as e:
        # ValueError is not expected, but we handle it cleanly.
        print('Fatal ValueError exception - {}'.format(str(e)))
        sys.exit(-1)

    # Now that the table dictionary is available, stitch the tables together.
    tables_written = stitch_tables(table_dict, output_directory_name)

    print('Processed and wrote {} tables to directory "{}"'.format(tables_written, "processed-metrics"))
    return 0


def make_list_of_files_with_timestamped_data(input_directory_name_list):
    """ Make a list of CSV files with timestamped data from the input directory list.

    Args:
        input_directory_name_list ([str]): The dictionary with key of table name and value of list
                                           of CSV objects for the table.

    Test:
        rehydrate_test.py (implicit)
    """""
    timestamped_files = []

    for directory in input_directory_name_list:
        # Create a dictionary with a key table name (which has .csv on the end) and a value of the list
        # of files that need to be combined for that table name.  This iterates over the subdirectories,
        # and only includes files that have timestamps as a column header.
        directory_name = os.path.abspath(directory)
        file_list = get_timestamped_files_list(directory_name)

        timestamped_files.extend(file_list)

    return timestamped_files


def stitch_tables(table_dict, output_directory_name):
    """ A dictionary entry has a key of table name and a value of a list of CSV objects comprising the
        table. So for each dictionary entry, rehydrate, delete the CSV object, stitch and trim the
        CSV objects together chronologically, and write the single table that spans the files from
        the collection directories to an output directory.

    Args:
        table_dict (dict): The dictionary with key of table name and value of list of CSV objects
                           for the table.

        output_directory_name (str): Directory location of the results.

    Test:
        rehydrate_test.py (implicit)
    """""
    # Iterate over each table name and process the list of files for that table.
    tables_written = 0

    # Adjust open arguments for Python2 vs 3
    if PY_VERSION_MAJOR > 2:
        open_mode = 'wt'
        kwargs = {'newline': ''}
    else:
        open_mode = 'wb'
        kwargs = {}

    for table_name, table_csv_obj_list in list(table_dict.items()):
        # Create the output file and write the resulting table values.
        filename_with_extension = table_name + '.csv'

        output_file_path_name = os.path.join(output_directory_name, filename_with_extension)

        # Create the csv file writer
        try:
            csv_output_file = io.open(output_file_path_name, open_mode, **kwargs)

        except IOError as err:
            raise ValueError('Unable create the file [{}].  Error: {}'.format(output_file_path_name, str(err)))

        file_writer = csv.writer(csv_output_file, delimiter=',')

        # Initialize the current end timestamp for each table that is processed
        current_end_timestamp = None

        # Stitch together all the tables in the list
        is_first_table = True

        for csv_obj in table_csv_obj_list:
            last_row = None

            # Read the header for this table object and find the column index of the timestamp value.
            headers = csv_obj.get_column_headers()
            if not headers:
                raise IndexError('CSV object for file "{}" does not have any rows'.format(csv_obj.filename))

            timestamp_index = get_column_header_indices(headers, [COLUMN_HEADER_TIMESTAMP_LOWER])[0]

            # Read the table into memory, sort it and insert the column header back into the sorted table
            all_rows = csv_obj.read_all_rows()
            sorted_table = sorted(all_rows[1:], key=operator.itemgetter(timestamp_index))

            # Only add the header back for the first table, the other tables
            # will be stitched in and the header is no longer needed
            if is_first_table:
                is_first_table = False

                # Identify and write the header.
                csv_obj.locate_header_indices(all_rows[0])
                file_writer.writerow(all_rows[0])

            for row in sorted_table:
                last_row = csv_obj.stitch(row, current_end_timestamp, timestamp_index, file_writer)

            try:
                # Update the end timestamp based upon the last item written to the file.
                if last_row:
                    current_end_timestamp = \
                        convert_string_time_to_datetime(last_row[timestamp_index])

            except ValueError as e:
                # Make sure to close the file writer if there is an exception.
                csv_output_file.close()

                # This should never happen because if CSV objects have data, they must have a timestamp
                # column (this is validated when data is added to the CSV object.
                raise ValueError('Could not get timestamp while stitching tables together:\n [{}]'.format(str(e)))

            tables_written += 1

        # Close the file writer
        csv_output_file.close()

    return tables_written


def create_output_directory(directory_name):
    """ Create the specified directory, and optionally delete it if it already exists.

        The first row on the input and output row are the column headers.

    Args:
        directory_name (str): The name of the directory to create, where output files get written.

    Raises OSError

    Test:
        rehydrate_test.py::test_okay_to_create_use_directory
    """""
    # Create a directory into which to write the results.
    try:
        os.mkdir(directory_name)

    except OSError as e:
        sys.stderr.write('Cannot create directory: {}, please delete the directory if you need new results.\n'
                         .format(str(e)))
        raise


def get_timestamped_files_list(directory_name):
    """ Returns a list of timestamped files in the specified directory or below.

    Args:
        directory_name (str): Current working directory. Caller can do this: directory_name = getcwd()

    Returns:
        The dictionary key (table name) has the value of the list of files to construct a unified
        table.

    Test:
        rehydrate_test.py (implicit)
    """""
    # This is the total list of csv files that have been found in the subdirectories.
    csv_files_list = []

    # This loop creates a filepath for every single CSV file, including the top level filenames.
    for subdir, _dirs, files in os.walk(directory_name):
        # Iterate through the subdirectories and get a list of CSV files in each directory.
        #
        # Python 2.7 is weak on handling unicode filenames. The code should use '.casefold', but it is
        # not available.
        csv_files_list.extend([os.path.join(subdir, filename) for filename in files
                               if os.path.splitext(filename)[1].lower() == '.csv'])

    # From the list of fully qualified filenames that are csv files, filter out files that do not
    # have timestamp entries and at least one data row.  The files without a timestamp column and at
    # least one row of data cannot be stitched together, so ignore them.
    only_timestamped_with_data_files = [full_file_name for full_file_name in csv_files_list
                                        if table_has_timestamped_data(full_file_name)]

    # Make sure the filenames are in sorted order.
    return sorted(only_timestamped_with_data_files)


def make_csv_file_dict(timestamped_files, catalog_obj):
    """ Make a dictionary with the table name as key and the value the list of files for that
        table name.

    Args:
        timestamped_files ([str]): The list of timestamped files to add to the dictionary.
                                   The files have already been pre-screened to be timestamped files
                                   with data.

        catalog_obj (Catalog): The catalog to use to look up seconds between intervals.

    Returns:
        The dictionary key (table name) has the value of the list of files to construct a unified
        able.

    Test:
        rehydrate_test.py (implicit)
    """""
    # Get a set of table names from the list of files.  Do this by generating a list of filenames
    # stripped of their path, and then converting that list to a set to get rid of the duplicates
    # that appear because of the multiple collections.
    #
    # The"list_of_names" looks like [a.csv, ab.csv, b.csv, b.csv, c.csv, c.csv, ...] so the set
    # looks like (a.csv, b.csv, c.csv).
    list_of_names = [os.path.basename(filename) for filename in timestamped_files]
    table_name_set = set(list_of_names)

    # Create a dictionary, with the key being the {tablename}.csv, and the value being the list of
    # files for that table, typically one per collection.
    csv_file_dict = build_table_name_to_csv_objs_dict(table_name_set,
                                                      timestamped_files,
                                                      catalog_obj)

    return csv_file_dict


def build_table_name_to_csv_objs_dict(table_name_set, files_with_timestamps, catalog_obj):
    """ This method receives a set of table names, and a list of files that are associated with the
        file names.  It builds a dictionary with the key of table name and a set of files that
        belong to that table name.

    Args:
        table_name_set (set): The set of table names, each with a set of associated files.
        files_with_timestamps ([str]): A list of all files with timestamps.
        catalog_obj (Catalog): The catalog object from which to get the interval seconds for a table.

    Returns:
        Timestamp that is "seconds_between_samples" later.

    Test:
        rehydrate_test.py (implicit)
    """""
    table_to_csv_obj_dict = dict()

    for csv_file in table_name_set:
        # Compute the key for the dictionary entry.  No need for the .csv to be included in the key.
        table_name = os.path.splitext(csv_file)[0]
        try:
            table_time_interval_secs = catalog_obj.get_time_interval_secs(table_name)

        except ValueError:
            # This error may not possible if the above always fills in a value.
            sys.stderr.write('No catalog or catalog entry for table {}, skipping the file.\n'.format(table_name))
            continue

        # Get a list of fully qualified names that contain this table name.
        file_list = [filename for filename in files_with_timestamps
                     if csv_file == os.path.basename(filename)]

        # Create a list of CSV Objects associated with each filename
        # No files have been loaded into memory at this point.  Use CsvObj.read_all_rows() to do that.
        csv_obj_list = [CsvObj(filename, table_time_interval_secs) for filename in file_list]
        for csv_obj in csv_obj_list:
            all_rows = csv_obj.read_all_rows()
            csv_obj.locate_header_indices(all_rows[0])

        # Drop all CSV objects that don't have data, they contribute no data.
        csv_list_with_data = [csv_obj for csv_obj in csv_obj_list if csv_obj.has_data()]

        # The dictionary key (table name) has the value of the list of files to construct a unified
        # table.
        table_to_csv_obj_dict[table_name] = csv_list_with_data

    return table_to_csv_obj_dict


def next_timestamp(current_timestamp_str, seconds_between_samples):
    """ A generator that generates the next timestamp.  Is initialized with the first timestamp from
        which to get a second timestamp.

    Args:
        current_timestamp_str (str): The timestamp that is the base.  The first value returned by
                                     the generator is the timestamp after this one.
                                     Example format is: 2019-05-23 14:30:00+00:00

        seconds_between_samples (int): The number of seconds later that the generated timestamp will be.

    Returns:
        Timestamp that is "seconds_between_samples" later.

    Test:
        rehydrate_test.py (implicit)
    """""
    # Convert the time as a string to a time as a datetime.
    timestamp_datetime = convert_string_time_to_datetime(current_timestamp_str)

    while True:
        # Advance the time
        timestamp_datetime += timedelta(seconds=seconds_between_samples)

        # Convert the time from datetime back to the string format that the caller wants.
        current_timestamp_str = timestamp_datetime.strftime('%Y-%m-%d %H:%M:%S+00:00')
        yield current_timestamp_str


class CsvObj(object):
    """Process a CSV metrics file with dedup (repeat_count) if necessary"""
    def __init__(self, filename=None, seconds_between_samples=TWENTY_SECONDS):
        """ Given CSV table represented by a list of table rows, rehydrate and reverse the table as
            needed.

            The first row on the input and output row are the column headers.

        Args:
            filename (str): The name of the file to rehydrate.  May be None, mostly for testing purposes.
            seconds_between_samples (int): Should an entry be rehydrated, it is rehydrated with this
                                           value of seconds between the previous entry.
        Returns:
            rehydrated table.

        Test:
            rehydrate_test.py::test_csv_handle_empty_file
        """""
        self._seconds_between_samples = seconds_between_samples
        self.line_index = 0  # CVS file line index
        self.filename = filename

        # These indices are for indexing into the row to get the value for a column.
        self._timestamp_index = None
        self._repeat_index = None
        self._deleted_index = None

    def read_all_rows(self):
        """
        Read the csv file from it's current position.

        Returns:
             A list of rows each of which is a list of values representing one row of data in the file or
             an empty list when the end of the file is reached.
        """""
        # The result of reading will be stored in the list called all_rows
        all_rows = []

        if self.filename:
            try:
                with io.open(self.filename, 'r', encoding='UTF-8') as file_handle:
                    reader = csv.reader(file_handle, delimiter=',')

                    # Read each line from the file into memory
                    for row in reader:
                        all_rows.append(row)

            except IOError as err:
                raise ValueError('Unable to read from file [{}].  Error: {}'.format(self.filename, str(err)))

            except StopIteration:
                all_rows = []

        return all_rows

    def locate_header_indices(self, header_row):
        """ This assumes that the header row contains the column headers for the associated table.
        Read the header and locate the index for the timestamp, repeat count, and delete value

        Args:
            header_row ([str]): An array column of data containing the column names.

        Test:
            rehydrate_test.py::test_process_and_add_row
        """""
        if not header_row:
            return

        # The first row being added must be a column header.
        try:
            self._timestamp_index, self._repeat_index, self._deleted_index = CsvObj.process_header_row(header_row)

        except ValueError as e:
            raise ValueError('The first row is not a valid column header: {}'.format(str(e)))

    def stitch(self, data_row, current_end_timestamp, timestamp_index, file_writer):
        """ Process the row of data, which includes dropping if it has the 'deleted' column set to True,
            or rehydrating if the 'repeat_count' is set to True.

            The row must include a timestamp value.

            Stitch together a list of files that represent a table from oldest entry to newest entry.
            The first table is always taken in its entirety, but the rest of the tables may be either
            completely discarded, have some of the start discarded, or have all of their entries taken.

            B1: Should never happen since the table is presorted and all of its entries are discarded
                because none are older than the last entry in preceding table A.

            B2: All entries are discarded because none are older than the last entry in preceding table A.

            B3: Only the some of the last entries from B3 are used, the ones older than the last entry
                in the preceding table A.

            B4: All entries are taken as all are older than the last entry in preceding table A.

                       +----+
                       | B1 |
                       +----+
             +-------+
             |       |        +----+
             |   A   |        | B2 |
             |       |        |    | +----+
             +-------+        +----+ | B3 |
                                     |    |
                                     +----+ +----+
                                            | B4 |
                                            |    |
                                            +----+

        Args:
            data_row ([str]): An array column data for this row
            current_end_timestamp ():
            timestamp_index (int): The column index in the row associated with the timestamp value.
            file_writer (): A csv file handle.

        Raises ValueError

        Test:
            rehydrate_test.py::test_process_and_add_row
        """""
        last_row = None

        # Some rows are marked as 'deleted' and shall be ignored as they don't contain useful data.
        if self._deleted_index is not None and data_row[self._deleted_index]:
            # This row is marked as deleted, do not process it.
            return last_row

        # Get the repeat count to determine if row of data should be duplicated repeat_count-1 times.
        if self._repeat_index is not None:
            # repeat_count column exists

            if data_row[self._repeat_index] == '':
                # MDT-91238 log and drop rows with empty repeat count
                sys.stderr.write('Row {} of {} has an empty repeat_count value, drop line:\n"{}"\n'.
                                 format(self.line_index, self.filename, ','.join(data_row)))
                return last_row

            repeat_count = int(data_row[self._repeat_index])

            # All subsequent rehydrated rows must have a repeat_count value of 1.
            data_row[self._repeat_index] = '1'
        else:
            # This case should never be reached since all metrics tables are supposed to have this field.
            repeat_count = 1

        # Insert the row at the end of the table.  The order doesn't matter here, because the
        # table will need to be re-ordered because it is not reordered before this processing
        # occurs.

        # Stitching is performed by making sure to only add rows after the current_end_timestamp
        if current_end_timestamp is None or \
           is_row_after_timestamp(data_row, current_end_timestamp, timestamp_index):
            file_writer.writerow(data_row)
            last_row = data_row

        # Repeat the row if necessary.
        if repeat_count > 1:
            # This row must be duplicated until its "instance count" matches the repeat count.
            last_row = self.rehydrate_row(data_row, repeat_count, current_end_timestamp,
                                          timestamp_index, file_writer)

        return last_row

    @staticmethod
    def process_header_row(column_header_row):
        """ Get the indices for timestamp, repeat and deleted.

        Args:
            column_header_row ([]): The column header row.

        Raises:
            ValueError

        Test:
            rehydrate_test.py::test_process_header_row
        """""
        # Get the index of the repeat and timestamp columns.
        timestamp_index, repeat_index, deleted_index = \
            get_column_header_indices(column_header_row,
                                      [COLUMN_HEADER_TIMESTAMP_LOWER,
                                       COLUMN_HEADER_REPEAT_COUNT,
                                       COLUMN_HEADER_DELETED])

        # Check for missing indices.
        if timestamp_index is None:
            # The timestamp is the necessary column for metrics files.  'Deleted' and
            # 'Repeat_count' are optional.
            raise ValueError('No timestamp column found')

        return timestamp_index, repeat_index, deleted_index

    def rehydrate_row(self, data_row, repeat_count, current_end_timestamp, timestamp_index, file_writer):
        """ Given CSV table represented by a list of table rows, rehydrate and reverse the table as
            needed.  On input, the first data row has the newest data, and on output the first data row
            has the oldest data.

            The first row on the input and output row are the column headers.

        Args:
            data_row ([str]): The row to expand.  It has a repeat count which needs to be converted.
            repeat_count (int): The number of times to repeat this row + 1 (e.g. if 2, then just create
                                one more entry.
            current_end_timestamp: DARRELL
            timestamp_index: DARRELL
            file_writer (): The csv_file handle

        Returns:
            rehydrated table.

        Test:
            rehydrate_test.py (implicit)
        """""
        # Write the row of data out to a file
        last_row = data_row

        later_generator = next_timestamp(data_row[self._timestamp_index], self._seconds_between_samples)

        copied_row = data_row
        for _ in range(repeat_count - 1):
            # Get a copy of the data row.
            new_row = copied_row

            # Advance the timestamp for this new row using a generator.
            new_row[self._timestamp_index] = next(later_generator)

            # Test to see if the rehydrated timestamp value should be included
            if current_end_timestamp is None or \
               is_row_after_timestamp(data_row, current_end_timestamp, timestamp_index):
                # This row needs to be included, write the hydrated row.
                last_row = new_row
                file_writer.writerow(new_row)

        # Return the last row written in order to keep track of the last timestamp to stitch
        return last_row

    def sort_table_with_timestamp(self, rows):
        """ Sort a table according to its timestamp index column.  The first row of the table is a
            column header row, and shall not be included in the sort, and shall remain the first row
            in the table after the sort.

        Test:
            rehydrate_test.py (implicit)
        """""
        # Sort the table according to a specific column, in this case a timestamp column.
        sorted_table = sorted(rows[1:], key=operator.itemgetter(self._timestamp_index))

        # Insert the column header back in.
        sorted_table.insert(0, rows[0])

        return sorted_table

    def read_first_two_rows(self):
        """ Read the beginning of the csv file and return the first two rows

        Exceptions:
            ValueError if the file could not be opened.

        Returns:
             (header[str], first_row[str]) Two lists of strings.
             The first represents the column headers and the second contains the first row of data
             The lists will be empty if it failed to read the first two rows.
        """""
        header = []
        first_row = []
        if not self.filename:
            return header, first_row

        try:
            with io.open(self.filename, 'r', encoding='UTF-8') as file_handle:
                reader = csv.reader(file_handle)
                try:
                    # Read the column header
                    header = next(reader)

                    # Try to read one line of data
                    first_row = next(reader)

                    # A row of data exists to reach this code point.
                    return header, first_row

                except StopIteration:
                    # The file did not have a row of data. Swallow the exception and continue.
                    return header, first_row

        except IOError:
            raise ValueError('Could not open file {}'.format(self.filename))

    def read_last_row(self):
        """
        Read the csv file and return the last row.  This is used to obtain the last seen timestamp.

        Return:
             last_row[str] - A list of string values or an empty list
        """""
        last_row = []
        if not self.filename:
            return last_row

        try:
            with io.open(self.filename, 'r', encoding='UTF-8', errors='ignore') as file_handle:
                all_rows = file_handle.readlines()
                if all_rows:
                    last_row = all_rows[-1]

                    # last_row is an unicode line, convert it to a string, remove the '\n'
                    # and then convert it to a list of strings
                    last_row = last_row.encode('ascii', 'ignore').strip().split(',')

        except IndexError as err:
            raise ValueError("Attempted to read the past the last row {}. Error: {}".format(last_row, str(err)))

        except IOError:
            raise ValueError('Could not open file {}'.format(self.filename))

        return last_row

    def has_data(self):
        """ Return True if the table has data, i.e. at least two rows, since one is a header row.

        Test:
            rehydrate_test.py::test_csv_handle_empty_file
        """""
        header, first_row = self.read_first_two_rows()
        return True if header and first_row else False

    def get_column_headers(self):
        """ Get only the row which contains the column headers.  Helps with testing.

        Test:
            rehydrate_test.py::test_stitch_some_overlap
        """""
        header, _ = self.read_first_two_rows()
        return header

    def _add_rows(self, rows_to_add, writer):
        """ Add rows to this table.  Useful for testing so that data can be injected without
            having to create a file.

        Raises ValueError

        Test:
            rehydrate_test.py::test_stitch_no_overlap
        """""
        # It is not guaranteed that the added rows would be sorted, so sort them now.
        sorted_table = self.sort_table_with_timestamp(rows_to_add)

        for row in sorted_table[1:]:
            self.stitch(row, None, self._timestamp_index, writer)

    def get_first_timestamp_as_dt(self):
        """ Get the first timestamp in this table.  Needed for sorting the tables according to
            starting timestamp.

        Test:
            rehydrate_test.py::test_get_first_timestamp_as_dt
        """""
        header, first_row = self.read_first_two_rows()

        # Neither of these cases can happen, as tables of this type would not have been added.
        if (not first_row) or (self._timestamp_index is None):
            raise ValueError('No timestamp column found in {} for first timestamp'.format(self.filename))

        # Convert to a datetime format so that dates can be more easily compared.
        timestamp = first_row[self._timestamp_index]
        return convert_string_time_to_datetime(timestamp)

    def get_last_timestamp_as_dt(self):
        """ Get the last timestamp in this table.  Needed for stitching tables together, to figure
            out where to start taking data from the next table.

        Raises ValueError

        Test:
            rehydrate_test.py::test_get_last_timestamp_as_dt
        """""
        last_row = self.read_last_row()

        # There is no last timestamp if there isn't at least one row with a timestamp.
        if (not last_row) or (self._timestamp_index is None):
            raise ValueError('No timestamp column found in {} for last timestamp'.format(self.filename))

        # Convert to a datetime format so that dates can be more easily compared.
        timestamp = last_row[self._timestamp_index]
        return convert_string_time_to_datetime(timestamp)

    def get_rows_after_timestamp(self, timestamp_dt, rows):
        """ Get all of the rows in a table after the specified timestamp.  Needed for stitching,
            for selecting the table data to be added to a previously processed table (which has
            an end timestamp).

        Test:
            rehydrate_test.py (implicit)
        """""
        if timestamp_dt is None:
            timestamp_dt = datetime.min

        # This assumes that the rows are sorted by timestamp.
        return [row for row in rows[1:]
                if convert_string_time_to_datetime(row[self._timestamp_index]) > timestamp_dt]


def is_row_after_timestamp(row, timestamp_dt, timestamp_index):
    """ Given a row of data and an index for the timestamp column,
    determine if the row is after the provided timestamp.

    Args:
        row ([str]): A single row of data from the csv file
        timestamp_dt (): A timestamp to compare against
        timestamp_index (int): The index into the row data to the specified timestamp column.

    Returns:
        bool: True if the row is after the specified timestamp.
    """""
    if timestamp_dt is None:
        timestamp_dt = datetime.min

    return convert_string_time_to_datetime(row[timestamp_index]) > timestamp_dt


def get_rows_after_timestamp(rows, timestamp_dt, timestamp_index):
    """ Get all of the rows in a table after the specified timestamp.  Needed for stitching,
        for selecting the table data to be added to a previously processed table (which has
        an end timestamp).

    Args:
        rows ([str]): A list of rows
        timestamp_dt:
        timestamp_index (int): An index where the timestamp value can be found in the list of rows

    Test:
        rehydrate_test.py (implicit)
    """""
    if timestamp_dt is None:
        timestamp_dt = datetime.min

    # This assumes that the rows are sorted by timestamp.
    return [row for row in rows
            if convert_string_time_to_datetime(row[timestamp_index]) > timestamp_dt]


def table_has_timestamped_data(filename):
    """ Determine if this is a timestamped table by checking if the first row (the column
        headers has an entry timestamp in it, and there is at least one row of data.

    Args:
        filename (str): The filename to check.

    Returns:
        True if table has a column 'timestamp' or 'Timestamp', and has one row of data.

    Raises IOError

    Test:
        rehydrate_test.py::test_table_has_timestamped_data
    """""
    try:
        with open(filename, 'r', encoding='UTF-8') as file_handle:
            reader = csv.reader(file_handle)
            try:
                column_header_row = next(reader)
                timestamp_index, = get_column_header_indices(column_header_row, [COLUMN_HEADER_TIMESTAMP_LOWER])
                if timestamp_index is None:
                    return False

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


def get_column_header_indices(column_headers, columns):
    """ Get the index of the timestamp column so that the timestamp value can be easily retrieved
        later.  Accounts for the fact that the timestamp may not be lowercase.

    Args:
        column_headers ([str]): The column header row to search.
        columns ([str]): The column header names to search for and include in the returned tuple.

    Returns:
        A tuple with entry for each requested column, with value None or index into the column header.
        Note that if you are getting one tuple, you must access correctly.  Choices are:
           timestamp, = get_column_header_indices(column_headers, columns),   [more Pythonic]
               or
           timestamp = get_column_header_indices(column_headers, columns)
           if timestamp_index[0] is not None:

    Test:
        rehydrate_test.py::test_get_column_header_indices
    """""
    # Get the column header row in lower case so that comparisons don't fail because of case.
    header_row_lower = [item.lower() for item in column_headers]

    return tuple(header_row_lower.index(column) if column in header_row_lower else None for column in columns)


def convert_string_time_to_datetime(time_str):
    """ Determine if this is a timestamped table by checking the provided column header row has
        timestamp column

        Flexible string conversion, will parse optional microseconds and/or timezone (which will be ignored)
        Timestamps are always assumed to be UTC regardless of a timezone field, and metrics record resolution
        does not require microseconds.

    Args:
        time_str (str): The time represented as a string, can have timezone.
                        The format of the timestamp is: 2019-05-23 15:35:20.000000+00:00
                        Optionally a timestamp may not contain the microseconds and/or timezone field.

    Returns:
        A datetime value

    Exceptions:
        ValueError if the timestamp is not the right format.

    Test:
        rehydrate_test.py::test_convert_string_time_to_datetime
    """""
    timestamp_no_timezone_str = time_str.split('+')[0].split('.')[0]  # split and ignore any timezone or microseconds

    return datetime.strptime(timestamp_no_timezone_str, '%Y-%m-%d %H:%M:%S')


class Catalog(object):
    """ The catalog object supports catalog operations, including initializing and retrieving values.
        It handles determining when an error should raise an exception, and when a default value
        should instead be returned.

    """""
    def __init__(self, catalog_filename):
        """ Initializes the catalog object.

        Args:
            catalog_filename (str): The catalog file name to read.

        Raises IOError

        Test:
            rehydrate_test.py::test_get_time_interval_secs
        """""
        self._catalog = None

        # Read the JSON catalog into a dictionary.
        try:
            self._catalog = json.loads(open(catalog_filename).read())

        except IOError:
            # The flag for using the default value is already set.
            sys.stderr.write('Using table named interval time without catalog when rehydrating\n')

    def get_time_interval_secs(self, table_name):
        """ Get a timestamp value to use for this table.  If all goes well, the value comes from a table
            entry.  But if value cannot be found, return a default value.

        Args:
            table_name(str): The table name with no directory, no suffix.

        Returns:
            The tables sample interval in seconds.

         Test:
            rehydrate_test.py::test_get_time_interval_secs
        """""
        # The catalog is either used completely, or the algorithm for using the name is used completely.
        if self._catalog is None or table_name not in self._catalog or 'interval_seconds' \
           not in self._catalog[table_name]:
            # The catalog does not exist, so get the interval for the table from the name.
            return Catalog._get_interval_from_table_name(table_name)
        else:
            # This is the happy path, a value is available.
            return self._catalog[table_name]['interval_seconds']

    @staticmethod
    def _get_interval_from_table_name(table_name):
        filename_ending_to_interval_secs = {
            "_twenty_seconds": TWENTY_SECONDS,
            "_five_mins": FIVE_MINUTES_IN_SECS,
            "_one_hour": ONE_HOUR_IN_SECS,
            "_one_day": ONE_DAY_IN_SECS,
        }

        interval_seconds = next((interval_seconds for key, interval_seconds
                                 in list(filename_ending_to_interval_secs.items())
                                 if table_name.endswith(key)),
                                TWENTY_SECONDS if not table_name.startswith('space') else FIVE_MINUTES_IN_SECS)

        return interval_seconds


if __name__ == '__main__':
    main(sys.argv[1:])
