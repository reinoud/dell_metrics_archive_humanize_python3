#!/usr/bin/env python
# -*- coding: utf-8 -*-
###########################################################################
# Copyright (C) Dell EMC 2020-2022
# All rights reserved.
# Licensed material -- property of Dell EMC
#
# humanize_node.py
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
from humanize_base import Humanize, get_full_path_for_file, get_value_from_key,  COLUMN_HEADER_NAME, \
    COLUMN_HEADER_APPLIANCE_ID
from humanize_appliance import APPLIANCE_FILENAME, SERIAL_NUMBER, TYPE, MODEL, SERVICE_TAG, MODE

COLUMN_HEADER_NODE_ID = 'node_id'


class HumanizeNode(Humanize):
    # Name of file with node details.
    NODE_FILENAME = 'node.csv'

    node_lookup_dict = dict()
    appliance_lookup_dict = dict()
    appliance_name_lookup_dict = dict()

    def __init__(self, input_directory=None):
        super(HumanizeNode, self).__init__(input_directory)

        # Get list of all CSV files with node details.
        node_csv_files = self.create_file_list(input_directory, self.NODE_FILENAME)

        # Create lookup for node.
        self.node_lookup_dict = self.perform_nested_lookup(node_csv_files,
                                                           [COLUMN_HEADER_NAME, COLUMN_HEADER_APPLIANCE_ID])

        # From the input directories, get list of all appliance CSV files.
        appliance_csv_files = self.create_file_list(input_directory, APPLIANCE_FILENAME)

        # Create dictionary with key as appliance id and value as a dictionary of values mentioned in the list
        self.appliance_lookup_dict = self.perform_nested_lookup(appliance_csv_files, [SERIAL_NUMBER, TYPE, MODEL,
                                                                                      SERVICE_TAG, MODE])
        # Create appliance name lookup
        self.appliance_name_lookup_dict = self.perform_lookup(appliance_csv_files)

    def add_details(self, filename):
        """
        A list is created with all entries in the file. Adds appliance name and model to each record that is mapped to
        appliance id from the file. It also adds appliance node name that is mapped with node id. Appends tx and rx per
        second value in MB.

        Args:
            filename (str): Name of metrics file that is to update.

        Returns:
           Number of records written in metric file.

        """""

        # Get full path for filename to process the metrics file
        input_filename = get_full_path_for_file(self.PROCESSED_METRICS_OUTPUT_DIR, filename)

        # Read data from appliance metrics file
        try:
            fe_eth_file_data = self.read_file_data(input_filename)

        except IOError:
            # Raise error if file is not processed.
            print("Could not process file {}".format(filename))
            return 0

        # Append appliance name to every record by using a lookup.
        if self.appliance_name_lookup_dict:
            fe_eth_file_data = self.perform_lookup_with_id(fe_eth_file_data, self.appliance_name_lookup_dict,
                                                           'appliance_name', COLUMN_HEADER_APPLIANCE_ID)

        # Append model details to every record in metrics file.
        if self.appliance_lookup_dict:
            fe_eth_file_data = self.add_model_details(self.appliance_lookup_dict, fe_eth_file_data)

        # Append appliance node details to every record in metrics file.
        if self.node_lookup_dict:
            fe_eth_file_data = self.add_node_details(fe_eth_file_data, self.node_lookup_dict, 'appliance_node_name')

        # Convert tx and rx per second values in MB
        data_with_tx_rx_details = self.calculate_tx_rx(fe_eth_file_data)

        fe_eth_records = self.write_to_file(filename, data_with_tx_rx_details)

        print('Records written in {} are {}'.format(filename, fe_eth_records))

        return fe_eth_records

    @staticmethod
    def add_model_details(appliance_lookup, file_data):
        """
        Adds appliance model details in fe eth node metrics file for every record using appliance id for lookup.

        Args:
            appliance_lookup (dict {}): A dictionary with key as appliance id and value as appliance details.
            file_data (list []): List of all records in metrics file.

        Returns:
            file_data with appliance model details

        Test:
            test_humanize_base.py::test_add_model_details
        """""
        # Append header for model name.
        file_data[0].append(MODEL)

        # Index for appliance id
        appliance_id_index = file_data[0].index(COLUMN_HEADER_APPLIANCE_ID)
        for data in file_data[1:]:
            try:
                # Get model name using appliance id from the lookup.
                model_name = appliance_lookup.get(data[appliance_id_index], data[appliance_id_index]).get('model')

            except AttributeError:
                # Exception is raised when appliance id does not exist,
                # default value returned is appliance id which is a
                # string. Trying to access model name from an appliance which does not exist will give an error.
                model_name = data[appliance_id_index]

            # Check if model name exists before appending the value. If not append appliance id
            data.append(model_name) if model_name else data.append(data[appliance_id_index])

        return file_data

    def add_fe_fc_node_details(self, filename):
        """
        A list of records is created from fe fc node metrics file. Adds appliance and node name using node id
        for lookup.  Calculates user friendly sizes and rates for every record in metrics file.

        Args:
            filename (str): Name of metrics file that is to update.

        Returns:
           Number of records written in metric file.

        Exception:
            ValueError if metric file to update is not found.
        """""
        input_filename = get_full_path_for_file(self.PROCESSED_METRICS_OUTPUT_DIR, filename)

        # Read data from fe fc node metrics file.
        try:
            fe_fc_file_data = self.read_file_data(input_filename)

        except IOError:
            # Return if file could not be processed.
            print("Could not process file {}".format(filename))
            return 0

        # Add appliance id and node name for every record.
        data_with_appliance_and_node = self.add_appliance_and_node_name(fe_fc_file_data, self.node_lookup_dict)

        # Read size in KiB, write size in KiB and IO size in KiB is calculated for each record.
        data_with_kib_calculations = self.calculate_kib_for_fe_fc_node(data_with_appliance_and_node)

        # For each entry in metrics file total bandwidth, read bandwidth and write bandwidth is calculated in MiBPS.
        data_with_mib_calculations = self.calculate_mib_for_fe_fc_node(data_with_kib_calculations)

        # For each entry in metrics file calculate unaligned read, write and total bandwidth in MiBPS.
        data_with_unaligned_details = self.add_unaligned_details(data_with_mib_calculations)

        # Once the records are updated it is written back to metrics file.
        records_written = self.write_to_file(filename, data_with_unaligned_details)

        print('Records written in {} are {}'.format(filename, records_written))

        return records_written

    def add_metrics_node_details(self, filename):
        """
        Adds node name in node metrics file using node id for lookup.

        Args:
            filename (str): Name of metrics file that is to update.

        Returns:
           Number of records written in metric file.

        Exception:
            ValueError if metric file to update is not found.
        """""
        if not self.node_lookup_dict:
            return 0

        input_filename = get_full_path_for_file(self.PROCESSED_METRICS_OUTPUT_DIR, filename)

        # Read data from appliance metrics file
        try:
            node_file_data = self.read_file_data(input_filename)

        except IOError:
            # Return if file could not be processed.
            print("Could not process file {}".format(filename))
            return 0

        # Add node name using node lookup for each record.
        file_data_with_node_name = self.add_node_details(node_file_data, self.node_lookup_dict, 'node_name')

        # Once the records are updated it is written back to metrics file.
        node_records = self.write_to_file(filename, file_data_with_node_name)

        print('Records written in {} are {}'.format(filename, node_records))

        return node_records

    def add_node_details(self, file_data, node_lookup, header_name):
        """
        Adds node details for every record in metrics file using node lookup. Header is added for node details as well.

        Args:
            file_data (list []): List of all records in metrics file.
            node_lookup (dict {}): A dictionary with node id as key and node details as lookup.
            header_name (str): Header name to append.

        Test:
            test_humanize_base.py::test_add_node_details
        """""
        # Append header to header row.
        file_data[0].append(header_name)

        # Get node id index.
        node_id_index = file_data[0].index(COLUMN_HEADER_NODE_ID)

        # For every record append node name using node lookup.
        for data in file_data[1:]:
            node_name = get_value_from_key(data[node_id_index], node_lookup, COLUMN_HEADER_NAME)
            data.append(node_name)

        return file_data

    @staticmethod
    def add_appliance_and_node_name(fe_fc_file_data, node_lookup):
        """
        Adds appliance id and node name by performing a lookup using node_id to each record in file.

        Args:
            fe_fc_file_data(list[]): A list of records in metric file data.
            node_lookup(dict{}):  A dictionary with key as node id and value as appliance id and node name.

        Returns:
            A list of file records with appliance id and node name.

        Test:
            test_humanize_base.py::test_add_appliance_and_node_name.
        """""
        fe_fc_file_data[0].extend(['appliance', COLUMN_HEADER_NAME])

        # Get index for node id.
        node_id_index = fe_fc_file_data[0].index(COLUMN_HEADER_NODE_ID)

        for data in fe_fc_file_data[1:]:
            app_id = get_value_from_key(data[node_id_index], node_lookup, COLUMN_HEADER_APPLIANCE_ID)
            data.append(app_id)

            node_name = get_value_from_key(data[node_id_index], node_lookup, COLUMN_HEADER_NAME)
            data.append(node_name)

        return fe_fc_file_data

    def calculate_kib_for_fe_fc_node(self, fe_fc_file_data):
        """
        Converts bytes to KB for fe fc node metric file. Appends headers for each Kb details calculated.

        Args:
            fe_fc_file_data (list[]): A list of records in metrics file.

        Returns:
            A list of file records with bytes converted to KB.

        Test:
            test_humanize_base.py::test_calculate_kib_for_fe_fc_node
        """""
        file_data = namedtuple('FileData', fe_fc_file_data[0])

        for record in fe_fc_file_data[1:]:
            data = file_data(*record)

            try:
                read_size_kib = self.convert_to_kib(round(float(data.avg_read_size), 2), 2)
            except ValueError:
                read_size_kib = 0

            try:
                write_size_kib = self.convert_to_kib(round(float(data.avg_write_size), 2), 2)
            except ValueError:
                write_size_kib = 0

            try:
                io_size_kib = self.convert_to_kib(round(float(data.avg_io_size), 2), 2)
            except ValueError:
                io_size_kib = 0

            record.extend([read_size_kib, write_size_kib, io_size_kib])

        fe_fc_file_data[0].extend(['read_size_KiB', 'write_size_KiB', 'io_size_KiB'])

        return fe_fc_file_data

    def calculate_mib_for_fe_fc_node(self, fe_fc_file_data):
        """
        Converts bandwidth values from bytes to MB per second in metric files.

        Args:
            fe_fc_file_data(list[]): List of records in metric file data.

        Returns:
            A list of records with bandwidth values converted from bytes to MB per second.
        """""
        file_data = namedtuple('FileData', fe_fc_file_data[0])

        for record in fe_fc_file_data[1:]:
            data = file_data(*record)

            try:
                read_mibps = self.convert_to_mib(data.read_bandwidth, 2)
            except ValueError:
                read_mibps = 0

            try:
                write_mibps = self.convert_to_mib(data.write_bandwidth, 2)
            except ValueError:
                write_mibps = 0

            try:
                total_mibps = self.convert_to_mib(data.total_bandwidth, 2)
            except ValueError:
                total_mibps = 0

            record.extend([read_mibps, write_mibps, total_mibps])

        fe_fc_file_data[0].extend(['read_MiBPS', 'write_MiBPS', 'total_MiBPS'])

        return fe_fc_file_data

    def add_unaligned_details(self, fe_fc_file_data):
        """
        Adds unaligned details from the fe fc metrics file. User friendly rates are added converting bytes per second
        to megabytes per second. Unaligned read, write and total bandwidth is equated to user friendly values.

        Args:
            fe_fc_file_data (list): List of records in metrics file.

        Returns:
            A list of records with unaligned read, write and total bandwidth details.

        Test:
            test_humanize_base.py::test_add_unaligned_details
        """""
        file_data = namedtuple('FileData', fe_fc_file_data[0])

        for record in fe_fc_file_data[1:]:
            # Convert each record to namedtuple before processing.
            data = file_data(*record)

            unaligned_read_bandwidth_mibps = self.convert_to_mib(data.unaligned_read_bandwidth, 2)
            record.append(unaligned_read_bandwidth_mibps)

            unaligned_write_bandwidth_mibps = self.convert_to_mib(data.unaligned_write_bandwidth, 2)
            record.append(unaligned_write_bandwidth_mibps)

            unaligned_bandwidth_mibps = self.convert_to_mib(data.unaligned_bandwidth, 2)
            record.append(unaligned_bandwidth_mibps)

        fe_fc_file_data[0].extend(['unaligned_read_bandwidth_MiBPS', 'unaligned_write_bandwdith_MiBPS',
                                   'unaligned_bandwidth_MiBPS'])

        return fe_fc_file_data
