#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###########################################################################
# Copyright (C) Dell EMC 2020-2022
# All rights reserved.
# Licensed material -- property of Dell EMC
#
# humanize_hardware.py
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
import operator
import re
from collections import namedtuple
from operator import add
from humanize_base import Humanize, get_full_path_for_file, get_value_from_key, COLUMN_HEADER_APPLIANCE_ID, \
    BYTES_CONVERTER

# Regex to extract drive_type from string.
# Eg: str = {u'firmware_version': u'GPJ99E5Q', u'size': 1920383410176, u'drive_type': u'NVMe_SSD}
DRIVE_TYPE_REGEX = '(?:u\'drive_type\': u\')(.*?)(?:\')'

COLUMN_HEADER_NODE_ID = 'node_id'


class HumanizeHardware(Humanize):
    # Name of file with hardware details.
    HARDWARE_FILENAME = 'hardware.csv'

    lookup_dict = dict()

    def __init__(self, input_directory):
        super(HumanizeHardware, self).__init__(input_directory)

        # From input directories create list of hardware files.
        hardware_csv_files = self.create_file_list(input_directory, self.HARDWARE_FILENAME)

        # Create a dictionary with key as the drive id and value as drive name and type.
        self.lookup_dict = self.create_hardware_lookup(hardware_csv_files)

    def add_details(self, filename):
        """
        A list of records is created from drive by node metrics file. Identifies drive type and location using hardware
        config file. The data is further sorted on basis of drive location and node id before further performing any
        metric calculation. Formulas are added to determine delta and divide by sample time ie. 20 seconds.

        Args:
            filename (str): Name of metrics file that is to update.

        Returns:
           Number of records written in metric file.

        """""
        if not self.lookup_dict:
            return 0

        input_filename = get_full_path_for_file(self.PROCESSED_METRICS_OUTPUT_DIR, filename)

        # Read data from appliance metrics file.
        try:
            file_data = self.read_file_data(input_filename)

        except IOError:
            # Return if file could not be processed.
            print("Could not process file {}".format(filename))
            return 0

        # Add drive location and type for all records in metric file.
        data_with_drive_details = self.add_drive_details(file_data, self.lookup_dict,
                                                         ['drive_location',
                                                          'drive_type'])

        # Sort data depending on drive location and node id.
        data_after_sorting = self.sort_drive_data(data_with_drive_details, COLUMN_HEADER_NODE_ID)

        # Add iops details for num_reads and num_writes and calculate the total_iops value.
        data_with_iops_details = self.add_iops_details(data_after_sorting, COLUMN_HEADER_NODE_ID)

        # Calculate delta for block_read and block_write and convert to MB.
        data_with_mbps_details = self.add_mbps_details(data_with_iops_details, COLUMN_HEADER_NODE_ID)

        # Calculate KB value using MB and IOPS value for each record.
        data_with_kb_details = self.add_kb_details(data_with_mbps_details)

        # Calculate RT details using response time and IOPS value for each record.
        data_with_rt_details = self.add_rt_details(data_with_kb_details, COLUMN_HEADER_NODE_ID)

        # Calculate queue details.
        data_with_queue = self.add_queue_details(data_with_rt_details, COLUMN_HEADER_NODE_ID)

        # Once the records are updated it is written back to metrics file.
        records_written = self.write_to_file(filename, data_with_queue)

        print('Records written in {} are {}'.format(filename, records_written))

        return records_written

    def add_metrics_by_drive_by_appliance_details(self, filename):
        """
        A list of records is created from drive by appliance metrics file. Identifies drive type and location using
        hardware
        config file. The data is further sorted on basis of drive location and appliance id before further performing
        any metric calculation. Formulas are added to determine delta and divide by sample time ie. 20 seconds.

       Args:
           filename (str): Name of metrics file that is to update.

        Returns:
           Number of records written in metric file.

        """""
        if not self.lookup_dict:
            return 0

        input_filename = get_full_path_for_file(self.PROCESSED_METRICS_OUTPUT_DIR, filename)

        # Read data from appliance metrics file.
        try:
            file_data = self.read_file_data(input_filename)

        except IOError:
            # Return if file could not be processed.
            print("Could not process file {}".format(filename))
            return 0

        # Add drive location and type for all records in metric file
        data_with_drive_details = self.add_drive_details(file_data, self.lookup_dict,
                                                         ['drive_location',
                                                          'drive_type'])

        # Sort data depending on drive location and appliance id.
        data_after_sorting = self.sort_drive_data(data_with_drive_details, COLUMN_HEADER_APPLIANCE_ID)

        # Add iops details for num_reads and num_writes and calculate the total_iops value.
        data_with_iops_details = self.add_iops_details(data_after_sorting, COLUMN_HEADER_APPLIANCE_ID)

        # Calculate delta for block_read and block_write and convert to MB.
        data_with_mbps_details = self.add_mbps_details(data_with_iops_details, COLUMN_HEADER_APPLIANCE_ID)

        # Calculate KB value using MB and IOPS value for each record.
        data_with_kb_details = self.add_kb_details(data_with_mbps_details)

        # Calculate RT details using response time and IOPS value for wach record.
        data_with_rt_details = self.add_rt_details(data_with_kb_details, COLUMN_HEADER_APPLIANCE_ID)

        # Calculate queue details.
        data_with_queue = self.add_queue_details(data_with_rt_details, COLUMN_HEADER_APPLIANCE_ID)

        # Once the records are updated it is written back to metrics file.
        records_written = self.write_to_file(filename, data_with_queue)

        print('Records written in {} are {}'.format(filename, records_written))

        return records_written

    def add_iops_details(self, file_data, metric_value):
        """
        Adds iops details to metric file records by calculating the delta between two consecutive records.
        Drive location and metric id are compared before performing any calculation.

        Args:
            file_data(list[]): A list of records from metric file.
            metric_value (str): Node id or appliance id to perform comparison before calculating the delta.

        Returns:
            A list of records with iops details calculated.

        Test:
            test_humanize_base.py::test_add_iops_details
        """""
        # Create namedtuple object using the header row.
        data = namedtuple('FileData', file_data[0])

        # Calculate read_iops for every record in metric file
        read_iops = [self.compare_and_calculate_iops(prev, curr, data, 'num_reads', metric_value) for
                     prev, curr in
                     zip(file_data[1:],
                         file_data[2:])]

        # Append calculated read_iops to metric file.
        file_data_with_read_iops = append_column_value(file_data, read_iops, 'read_iops')

        # Create namedtuple object after appending read_iops details.
        data = namedtuple('FileData', file_data[0])

        write_iops = [self.compare_and_calculate_iops(prev, curr, data, 'num_writes', metric_value) for
                      prev, curr in
                      zip(file_data_with_read_iops[1:],
                          file_data_with_read_iops[2:])]
        file_data_with_write_iops = append_column_value(file_data_with_read_iops, write_iops, 'write_iops')

        # Add read_iops and write_iops to get a list of total_iops for every record.
        total_iops = list(map(add, read_iops, write_iops))
        file_data_with_total_iops = append_column_value(file_data_with_write_iops, total_iops, 'total_iops')

        return file_data_with_total_iops

    def add_mbps_details(self, file_data, metric_value):
        """
        Adds mbps details to metric file records by calculating the delta between two consecutive records.
        Drive location and metric id are compared before performing any calculation.

        Args:
            file_data: A list of records from metric file.
            metric_value (str): Node id or appliance id to perform comparison before calculating the delta.

        Returns:
            A list of records with MB details calculated for each record.

        Test:
            test_humanize_base.py::test_add_mbps_details
        """""
        # Create namedtuple object using the header row.
        data = namedtuple('FileData', file_data[0])

        # Calculate read_mbps for every record in metric file.
        read_mbps = [self.compare_and_calculate_mbps(prev, curr, data, 'blocks_read', metric_value) for
                     prev, curr in
                     zip(file_data[1:],
                         file_data[2:])]

        # Append calculated read_mbps to metric file.
        file_data_with_read_mbps = append_column_value(file_data, read_mbps, 'read_mbps')

        # Create namedtuple object after appending read_mbps details.
        data = namedtuple('FileData', file_data[0])
        write_mbps = [self.compare_and_calculate_mbps(prev, curr, data, 'blocks_written', metric_value) for
                      prev, curr in
                      zip(file_data_with_read_mbps[1:],
                          file_data_with_read_mbps[2:])]

        file_data_with_write_mbps = append_column_value(file_data_with_read_mbps, write_mbps, 'write_mbps')

        # Add read_mbps and write_mbps to get a list of total_iops for every record.
        total_mbps = list(map(lambda x, y: round(x + y, 3), read_mbps, write_mbps))
        file_data_with_total_mbps = append_column_value(file_data_with_write_mbps, total_mbps, 'total_mbps')

        return file_data_with_total_mbps

    def add_kb_details(self, file_data):
        """
        Calculated KB details such as read and write size for every record.

        Args:
            file_data (list[]): A list of file data from the metrics file.

        Returns:
            A list of records with KB details calculated for each record.

        Test:
            test_humanize_base.py::test_add_kb_details
        """""
        # Create namedtuple object.
        data = namedtuple('FileData', file_data[0])

        # Calculate read size in KB.
        read_size = [self.calculate_kb_details(record, data, 'read_mbps', 'read_iops') for
                     record in file_data[2:]]

        # Append the calculated read size to the corresponding record.
        data_with_read_size_kb = append_column_value(file_data, read_size, 'read_size_KiB')

        # Create namedtuple object after appending read size to perform further processing.
        data = namedtuple('FileData', file_data[0])
        write_size = [self.calculate_kb_details(record, data, 'write_mbps', 'write_iops') for
                      record in data_with_read_size_kb[2:]]

        data_with_write_size_kb = append_column_value(data_with_read_size_kb, write_size, 'write_size_KiB')

        return data_with_write_size_kb

    def add_rt_details(self, file_data, metric_value):
        """
        Calculates read and write response time for every record in metric file by calculating the delta between two
        consecutive records. Drive location and metric id are compared before performing any calculation.

        Args:
            file_data (list{}): List of records in metric data.
            metric_value (str): Node id or appliance id to perform comparison before calculating the delta.

        Returns:
            A list of records with response time details.

        Test:
            test_humanize_base.py::test_add_rt_details
        """""
        # Create namedtuple object.
        data = namedtuple('FileData', file_data[0])

        # Calculate read response time for every record.
        read_rt = [
            self.compare_and_calculate_rt(prev, curr, data, 'cum_read_response_time', 'read_iops',
                                          metric_value)
            for
            prev, curr in
            zip(file_data[1:],
                file_data[2:])]

        # Append the calculated read response time to the corresponding record.
        data_with_read_rt = append_column_value(file_data, read_rt, 'read_rt')

        # Create namedtuple object after appending read rt to perform further processing.
        data = namedtuple('FileData', file_data[0])
        write_rt = [
            self.compare_and_calculate_rt(prev, curr, data, 'cum_write_response_time', 'write_iops',
                                          metric_value)
            for prev, curr in
            zip(data_with_read_rt[1:],
                data_with_read_rt[2:])]
        data_with_write_rt = append_column_value(data_with_read_rt, write_rt, 'write_rt')

        return data_with_write_rt

    def add_queue_details(self, file_data, metric_value):
        """
        Adds queue details to every record in metrics file. Drive location and metric id are compared before performing
        any calculation.

        Args:
            file_data(list[]): List of records from metrics file.
            metric_value (str): Node id or appliance id to perform comparison before calculating the delta.

        Returns:
            A list of records with queue details.
        """""
        # Create namedtuple object using the header row.
        data = namedtuple('FileData', file_data[0])

        # Calculate queue details by doing a metric id comparison.
        queue_details = [self.compare_and_calculate_queue(prev, curr, data, metric_value) for
                         prev, curr in
                         (zip(file_data[1:],
                              file_data[2:]))]

        # Append the calculated queue to corresponding record in file.
        data_with_queue = append_column_value(file_data, queue_details, 'queue')

        return data_with_queue

    @staticmethod
    def add_drive_details(file_data, id_lookup, header_list):
        """
        Adds drive location and drive type for every record in file by doing a lookup using drive id.

        Args:
            file_data(list[]): A list of records from the metric file.
            id_lookup(dict {}): A dictionary with key as id and value of drive type and location.
            header_list (list): A list of headers for drive_type and drive_location.

        Returns:
            A list of records with drive_type and drive_location.

        Test:
            test_humanize_base.py::test_add_drive_details
        """""
        # Append header list to file headers.
        file_data[0].extend(header_list)

        # Get the drive id index to perform lookup.
        drive_id_index = file_data[0].index('drive_id')
        for data in file_data[1:]:
            drive_location = get_value_from_key(data[drive_id_index], id_lookup, 'name')
            data.append(drive_location)

            drive_type = get_value_from_key(data[drive_id_index], id_lookup, 'type')
            data.append(drive_type)

        return file_data

    @staticmethod
    def sort_drive_data(file_data, metric_value):
        """
        Sorts file data first on drive location and then the metric value passed.

        Args:
            file_data (list{}): List of records from file.
            metric_value (str): node_id or appliance_id used to sort.

        Returns:
            Sorted list of data.

        Test:
            test_humanize_base.py::test_sort_drive_data
        """""

        # Get index of drive location.
        drive_location_index = file_data[0].index('drive_location')
        metric_index = file_data[0].index(metric_value)

        # Need to ensure adjacent rows are same drive location and metric value.
        file_data[1:] = sorted(file_data[1:], key=operator.itemgetter(drive_location_index, metric_index))

        if metric_value is COLUMN_HEADER_NODE_ID:
            appliance_index = file_data[0].index(COLUMN_HEADER_APPLIANCE_ID)
            file_data[1:] = sorted(file_data[1:], key=operator.itemgetter(appliance_index))

        return file_data

    @staticmethod
    def calculate_kb_details(row, data, mbps_value, iops_value):
        """
        Calculated Kb values for each record in file using the MBPS value and IOPS value.
        In case of an error it returns 0.
        This method is used to calculate both read_KB and write_KB.

        Args:
            row (list): Single row of data in the metric file.
            data: Object to create namedtuple.
            mbps_value (str): Mbps value.
            iops_value: IOPS value.

        Returns:
            KB value.

        Test:
            test_humanize_base.py::test_calculate_kb_details
        """""
        # Create a namedtuple object
        record = data(*row)
        try:
            # Return 0 if iops value is 0, otherwise calculate KB value using mb and iops value.
            return 0 if int(record.__getattribute__(iops_value)) == 0 else round(
                (record.__getattribute__(mbps_value) * BYTES_CONVERTER) / record.__getattribute__(iops_value), 2)

        except AttributeError:
            return 0

    @staticmethod
    def compare_and_calculate_iops(prev, curr, data, column, compare_value):
        """
        Determines the delta between two consecutive records to get iops value. If the compare value between two
        consecutive which can be either node id or appliance id is different then value returned is 0. If it is
        same then check whether drive location is same. If different value returned is 0. If compare value and drive
        location both are same then calculate the delta.

        Args:
            prev(list[]): Previous record used for comparison purposes.
            curr (list[]): Current record to get delta.
            data: Object to create namedtuple.
            column (str): Column Name whose value is used to calculate delta.
            compare_value (str): It is either 'node_id' or 'appliance_id'  depending upon the metric file it is used.

        Returns:
            iops value after comparison.

        Test:
            test_humanize_base.py::test_compare_and_calculate_iops
        """""
        # Convert list to namedtuple.
        prev_value = data(*prev)
        curr_value = data(*curr)

        try:
            # If compare value is same then check drive location. If they are different return 0
            # otherwise calculate delta.
            # __getattribute__ is used to get value of the column for which delta is calculated.
            if curr_value.__getattribute__(compare_value) == prev_value.__getattribute__(compare_value) and \
                    curr_value.drive_location == prev_value.drive_location:
                return (float(curr_value.__getattribute__(column)) - float(prev_value.__getattribute__(column))) / 20
            return 0

        # If column value is not found as one of the attributes then return 0.
        except AttributeError:
            return 0

    @staticmethod
    def compare_and_calculate_mbps(prev, curr, data, column, compare_value):
        """
        Determines the delta between two consecutive records to get mbps value. If the compare value between two
        consecutive which can be either node id or appliance id is different then value returned is 0. If it is
        same then check whether drive location is same. If different value returned is 0. If compare value and
        drive location both are same then calculate the delta.

        Args:
            prev(list[]): Previous record used for comparison purposes.
            curr (list[]): Current record to get delta.
            data: Object to create namedtuple.
            column (str): Column Name whose value is used to calculate mbps delta.
            compare_value (str): It is either 'node_id' or 'appliance_id'  depending upon the metric file it is used.

        Returns:
            mbps value after comparison.

        Test:
            test_humanize_base.py::test_compare_and_calculate_mbps
        """""
        # Convert list of record to namedtuple.
        prev_value = data(*prev)
        curr_value = data(*curr)

        try:
            # If compare value is same then check drive location.
            # If they are different return 0 otherwise calculate delta.
            # __getattribute__ is used to get value of the column for which delta is calculated.
            if curr_value.__getattribute__(compare_value) == prev_value.__getattribute__(compare_value) and \
                    curr_value.drive_location == prev_value.drive_location:
                return round((float(curr_value.__getattribute__(column)) -
                              float(prev_value.__getattribute__(column))) / 20 / 2 / BYTES_CONVERTER, 2)
            return 0
        # If column value is not found as one of the attributes then return 0.
        except AttributeError:
            return 0

    @staticmethod
    def compare_and_calculate_rt(prev, curr, data, response_time_value, iops_value, compare_value):
        """
        Determines the delta between two consecutive records to get rt value. If the compare value between two
        consecutive which can be either node id or appliance id is different then value returned is 0. If it is
        same then check whether drive location is same. If different value returned is 0. If compare value and
        drive location both are same then calculate the delta.

        Args:
            prev (list[]): Previous record used for comparison purposes.
            curr (list[]): Current record to get delta.
            data: Object to create namedtuple.
            response_time_value (str): Response time value to calculate rt.
            iops_value (str): Iops value to calculate rt.
            compare_value (str): It is either 'node_id' or 'appliance_id'  depending upon the metric file it is used.

        Returns:
            rt value calculated after comparisons.

        Test:
            test_humanize_base.py::test_compare_and_calculate_rt
        """""
        # Convert list of record to namedtuple.
        prev_value = data(*prev)
        curr_value = data(*curr)

        # Calculate difference between response time a value and iops value. If its an error return 0 else use it to
        # calculate rt value.
        try:
            value = (float(curr_value.__getattribute__(response_time_value)) - float(
                prev_value.__getattribute__(response_time_value))) / 20 / \
                    float(curr_value.__getattribute__(iops_value))
        except ZeroDivisionError:
            return 0
        else:

            # If compare value is same then check drive location.
            # If they are different return 0 otherwise calculate delta.
            # __getattribute__ is used to get value of the column for which delta is calculated.
            if curr_value.__getattribute__(compare_value) == prev_value.__getattribute__(compare_value) and \
                    curr_value.drive_location == prev_value.drive_location:
                return round(value, 2)
            return 0

    @staticmethod
    def create_hardware_lookup(csv_file_list):
        """
        Creates a lookup for drive type using appliance config file. The lookup is a dictionary with key as drive id
        and value is drive type. Drive is extracted from the extra details column in the config file. The base
        enclosure is either NVME_SSD, NVME_SCM or NVME_NVRAM. Hence value drive type will be data for SCM, SSD and
        nvram for NVRAM.

        Args:
            csv_file_list (list): List of hardware config files.

        Returns:
            A nested dictionary with key as id and value for drive location and drive type.

        Exceptions:
            ValueError if file is not accessible while creating dictionary for lookup.

        Test:
            test_humanize_base.py::test_create_hardware_lookup
        """""
        id_lookup = dict()

        # Process every config file in the list.
        for file_name in csv_file_list:
            try:
                with open(file_name) as file_handle:
                    reader = csv.reader(file_handle)
                    data = namedtuple('FileData', next(reader))

                    for row in reader:
                        file_data = data(*row)
                        key = file_data.id

                        # Extract the extra details column in every record.
                        extra_details = file_data.extra_details

                        # Capture value "drive_type"  using regex from extra details column.
                        drive_type_lst = re.search(DRIVE_TYPE_REGEX, extra_details)

                        # If the record is for drive details it will contain drive type details hence add it to the
                        # dictionary only if drive type exists.

                        if drive_type_lst:
                            # Split string on '_' as drive type has value in the format of NVME_SSD or NVME_NVRAM
                            drive_type_str = drive_type_lst.group(1).split('_')[1]

                            drive_type = 'data' if drive_type_str == 'SSD' or drive_type_str == 'SCM' else 'nvram'

                            id_lookup[key] = {'name': file_data.name,
                                              'type': drive_type
                                              }

            except IOError:
                raise ValueError('Could not open file {}'.format(file_name))

        return id_lookup

    @staticmethod
    def compare_and_calculate_queue(prev, curr, data, compare_value):
        """
        Determines the queue value by calculating delta using sum_arrival_queue_length and sum_arrival_queue_length.
        If the compare value which can be either node id or appliance id between two consecutive is different then
        value returned is 0. If it is same then check whether drive location is same. If different value returned is 0.
        If compare value and drive location both are same then calculate the queue.

        Args:
            prev (list[]): Previous record used for comparison purposes.
            curr (list[]): Current record to get delta.
            data: Object to create namedtuple.
            compare_value (str): It is either 'node_id' or 'appliance_id'  depending upon the metric file it is used.

        Returns:
            queue value after comparison.

        Test:
            test_humanize_base.py::test_compare_and_calculate_queue
        """""
        # Convert list of record to namedtuple.
        prev_value = data(*prev)
        curr_value = data(*curr)

        try:
            # Calculate the queue value.
            value = (float(curr_value.sum_arrival_queue_length) - float(prev_value.sum_arrival_queue_length)) / 20 / (
                    curr_value.read_iops + curr_value.write_iops)
        except ZeroDivisionError:
            return 0
        else:
            # If compare value is same then check drive location.
            # If they are different return 0 otherwise calculate delta.
            # __getattribute__ is used to get value of the column for which delta is calculated.
            if curr_value.__getattribute__(compare_value) == prev_value.__getattribute__(compare_value) and \
                    curr_value.drive_location == prev_value.drive_location:
                return round(value, 2)
            return 0


def append_column_value(file_data, column_data_to_append, column_header):
    """
    Append list of values to file data as a column. The list is calculated metrics that appends to file data.
    A header is also appended to the header column.

    Args:
        file_data(list[]): File data from the metrics file.
        column_data_to_append (list[]): Column appended to every row in file file data.
        column_header (str): Header appended to file data.

    Returns:
        A list of data with another column to append.

    Test:
        test_humanize_base.py::test_append_column_value
    """""

    # Append header to metrics file data for the column to add.
    file_data[0].append(column_header)

    # As the columns to append are calculated comparing values from previous row and since first row has no
    # values to
    # compare default value is set to 0.
    file_data[1].append(0)

    # Append calculated value to each row in file data.
    list(map(lambda x, y: x.append(y), file_data[2:], column_data_to_append))

    return file_data
