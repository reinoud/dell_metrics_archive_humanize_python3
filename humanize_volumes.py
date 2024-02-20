#!/usr/bin/env python
# -*- coding: utf-8 -*-
###########################################################################
# Copyright (C) Dell EMC 2020-2021
# All rights reserved.
# Licensed material -- property of Dell EMC
#
# humanize_volumes.py
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

from collections import namedtuple
from humanize_base import Humanize, get_full_path_for_file, VIRTUAL_VOLUME_FILENAME


class HumanizeVolumes(Humanize):
    """
    From the input directories, get list of all volume csv files. Volume details are written in multiple files
    depending if its a SAN or an HCI system. In case no volumes are created details are picked from
    nas_volume.csv files.
    """""
    # These are file names which has volume details.
    VOLUME_FILENAME = 'volume.csv'  # File name when volume details are present when volumes are created.
    NAS_VOLUME_FILENAME = 'nas_volume.csv'  # File name when volume details are present when volumes are not created.

    # Header for volume name to be appended in performance_metrics_by_volume.csv.
    COLUMN_HEADER_VOLUME_NAME = 'volume'
    COLUMN_HEADER_IO_SIZE_KIB = 'io_size_KiB'
    COLUMN_HEADER_READ_SIZE_KIB = 'read_size_KiB'
    COLUMN_HEADER_WRITE_SIZE_KIB = 'write_size_KiB'
    COLUMN_HEADER_TOTAL_MIB = 'total_MiBPS'
    COLUMN_HEADER_READ_MIB = 'read_MiBPS'
    COLUMN_HEADER_WRITE_MIB = 'write_MiBPS'

    # Index value to perform lookup in CSV file.
    VOLUME_ID = 'volume_id'

    lookup_dict = dict()

    def __init__(self, input_directory=None):
        super(HumanizeVolumes, self).__init__(input_directory)

        volume_csv_files = self.create_file_list(input_directory, self.VOLUME_FILENAME,
                                                 self.NAS_VOLUME_FILENAME, VIRTUAL_VOLUME_FILENAME)

        # Create a dictionary to assist lookup with volume id and volume name.
        self.lookup_dict = self.perform_lookup(volume_csv_files)

    def calculate_kib(self, volume_file_data):
        """
        Calculates average IO size, average read, average write size in KiB for each entry present in the metric file.
        The values are appended at the end of each record.

        Args:
            volume_file_data (list[]): Data from the volume metrics file for which calculations are performed.

        Returns:
             A list of entries with average IO, average read and average write in KiB appended at the end.
        """""
        volume = namedtuple('Volume', volume_file_data[0])

        for data in volume_file_data[1:]:
            volume_data = volume(*data)

            # Calculates average IO size in KiB
            io_size_in_kib = self.convert_to_kib(volume_data.avg_io_size, 1)
            data.append(io_size_in_kib)

            # Calculate average read size in KiB
            read_size_in_kib = self.convert_to_kib(volume_data.avg_read_size, 1)
            data.append(read_size_in_kib)

            # Calculate average write size in KiB
            write_size_in_kib = self.convert_to_kib(volume_data.avg_write_size, 1)
            data.append(write_size_in_kib)

        # Headers for IO size, read size and write size in KiB
        volume_file_data[0].extend(
            [self.COLUMN_HEADER_IO_SIZE_KIB, self.COLUMN_HEADER_READ_SIZE_KIB, self.COLUMN_HEADER_WRITE_SIZE_KIB])

        return volume_file_data

    def calculate_mib(self, volume_file_data):
        """
        Calculates total bandwidth, read bandwidth and write bandwidth  in MiB for each record in metrics file system
        and appends at the end of the record.

        Args:
            volume_file_data (list[]): Records present in the volume metrics file.

        Returns:
            A list with total, read and write bandwidth in MiB appended in the end for each record.
        """""

        volume = namedtuple('Volume', volume_file_data[0])

        for data in volume_file_data[1:]:
            volume_data = volume(*data)

            # Calculates total bandwidth in MiB
            total_mib = self.convert_to_mib(volume_data.total_bandwidth, 3)
            data.append(total_mib)

            # Calculates read bandwidth in MiB
            read_mib = self.convert_to_mib(volume_data.read_bandwidth, 3)
            data.append(read_mib)

            # Calculates write bandwidth in MiB
            write_mib = self.convert_to_mib(volume_data.write_bandwidth, 3)
            data.append(write_mib)

        # Header for total bandwidth in MiB
        volume_file_data[0].extend([self.COLUMN_HEADER_TOTAL_MIB, self.COLUMN_HEADER_READ_MIB,
                                    self.COLUMN_HEADER_WRITE_MIB])

        return volume_file_data

    def add_details(self, filename):
        """
        Adds volume name, user friendly rates converting bytes per second to megabytes per second,
        user friendly sizes from bytes to kilobytes in performance volume file. A list is created
        with all the entries in the file.  Volume name is appended at the end of each record depending
        upon the corresponding volume id. Columns are added to equate user friendly sizes, rates
        converting bytes to kilobytes and bytes per second to megabytes per second respectively.
        Once these calculations are done the list is written to the file with the help of a CSV writer.

        Args:
            filename (str): Name of metrics file that is to update.

        Returns:
            records_written: Records written in metric file system file.

        Exception:
            ValueError if file not found.

        Test:
            test_humanize_base.py::test_add_volume_details
        """""
        if not self.lookup_dict:
            print('No records available for volume lookup')
            return 0

        input_file = get_full_path_for_file(self.PROCESSED_METRICS_OUTPUT_DIR, filename)

        # A list of data from volume metrics file is created.
        try:
            file_data = self.read_file_data(input_file)

        except IOError:
            # Return if file could not be processed.
            print("Could not process file {}".format(filename))
            return 0

        # A lookup for volume id and name is performed and name is added at the end of each record.
        volume_file_data_with_name = self.perform_lookup_with_id(file_data, self.lookup_dict,
                                                                 self.COLUMN_HEADER_VOLUME_NAME, self.VOLUME_ID)

        # IO size in KiB, read size in KiB and write size in KiB is calculated and added for each record in the file.
        volume_file_data_with_kib = self.calculate_kib(volume_file_data_with_name)

        # For each entry in volume metrics file total bandwidth,
        # read bandwidth and write bandwidth is calculated in MiBPs
        volume_file_data_with_mib = self.calculate_mib(volume_file_data_with_kib)

        # Once the records are updated it is written back to metrics file.
        records_written = self.write_to_file(filename, volume_file_data_with_mib)

        print('Records written in {} are {}'.format(filename, records_written))
        return records_written
