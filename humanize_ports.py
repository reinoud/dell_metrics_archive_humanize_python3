#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###########################################################################
# Copyright (C) Dell EMC 2020-2021
# All rights reserved.
# Licensed material -- property of Dell EMC
#
# humanize_ports.py
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
import re
from collections import namedtuple, OrderedDict
from humanize_base import Humanize, get_full_path_for_file, get_value_from_key, COLUMN_HEADER_NAME

COLUMN_HEADER_INDEX = 'port_index'


class HumanizeEthPort(Humanize):
    # Name of file with eth port details
    ETH_PORT_FILENAME = 'eth_port.csv'

    # Name of file with virtual eth port details.
    VIRTUAL_ETH_PORT_FILENAME = 'veth_port.csv'

    # Regex to extract fe_port_id from string.
    # Eg: '{u'partner_id': u'23c38cd317d542b5bb1be6396ff6362f', u'fe_port_id': u'1776e139b5754be78a96908108001403'}'
    FE_PORT_ID_REGEX = '(?:u\'fe_port_id\': u\')(.*?)(?:\')'

    fe_eth_port_lookup_dict = dict()
    v_eth_port_lookup_dict = dict()

    def __init__(self, input_directory=None):
        super(HumanizeEthPort, self).__init__(input_directory)

        # From input directories create list of eth port files.
        eth_port_csv_files = self.create_file_list(input_directory, self.ETH_PORT_FILENAME)

        # From input directories create list if virtual eth port files.
        virtual_eth_port_csv_files = self.create_file_list(input_directory, self.VIRTUAL_ETH_PORT_FILENAME)

        # Create lookup for node.
        self.fe_eth_port_lookup_dict = self.perform_nested_lookup(eth_port_csv_files,
                                                                  ['name', 'current_speed'])

        # Create dictionary with key as virtual port id and value as name.
        self.v_eth_port_lookup_dict = self.create_virtual_port_lookup(virtual_eth_port_csv_files)

    def add_details(self, filename):
        """
        A list is created with all records from fe eth port metrics file. This method adds port name and speed details
        using fe eth port and virtual port lookups. It also calculates tx and rx per second for each record.

        Args:
            filename (str): Name of metrics file that is to update.

        Returns:
            Number of records written in metric file.

        Exception:
            ValueError if metric file to update is not found.
        """""
        if not self.fe_eth_port_lookup_dict and not self.v_eth_port_lookup_dict:
            return

        input_filename = get_full_path_for_file(self.PROCESSED_METRICS_OUTPUT_DIR, filename)

        # Read data from fe eth port metrics file
        try:
            fe_eth_file_data = self.read_file_data(input_filename)

        except IOError:
            # Return if file could not be processed.
            print("Could not process file {}".format(filename))
            return 0

        # Add port name and speed details. In case of virtual port and port speed as virtual.
        data_with_port_speed_details = self.add_port_and_speed_details(fe_eth_file_data, self.fe_eth_port_lookup_dict,
                                                                       self.v_eth_port_lookup_dict,
                                                                       ['port', 'speed'])

        # Convert tx and rx per second values in MB
        file_data_with_tx_rx_details = self.calculate_tx_rx(data_with_port_speed_details)

        # Return the number of records written in metrics file.
        fe_eth_records = self.write_to_file(filename, file_data_with_tx_rx_details)

        print('Records written in {} are {}'.format(filename, fe_eth_records))

        return fe_eth_records

    @staticmethod
    def add_port_and_speed_details(fe_eth_file_data, fe_eth_port_lookup, v_eth_port_lookup, header_list):
        """
        Adds port and speed details for fe eth port metrics file. If port id is virtual port name is selected from
        v_eth_port lookup and port speed is marked as "virtual". Append header column values for port name and speed.

        Args:
            fe_eth_file_data (list):  List of file data.
            fe_eth_port_lookup (dict{}): Dictionary for fe eth port.
            v_eth_port_lookup (dict{}): Dictionary for virtual eth port.
            header_list (list): List of headers.

        Returns:
            A list of data with port and speed details.

        Test:
            test_humanize_base.py::test_add_port_and_speed_details
        """""
        fe_eth_file_data[0].extend(header_list)

        fe_port_id_index = fe_eth_file_data[0].index('fe_port_id')

        # Process every record in the file.
        for data in fe_eth_file_data[1:]:
            # Append port name if it exist in fe eth port else lookup for virtual eth port.
            port_name = fe_eth_port_lookup.get(data[fe_port_id_index]).get(
                COLUMN_HEADER_NAME) if fe_eth_port_lookup.get(
                data[fe_port_id_index]) else v_eth_port_lookup.get(data[fe_port_id_index])

            data.append(port_name)

            # Append current speed if fe eth port else "virtual" for virtual eth port.
            speed = fe_eth_port_lookup.get(data[fe_port_id_index]).get(
                'current_speed') if fe_eth_port_lookup.get(
                data[fe_port_id_index]) else 'virtual'
            data.append(speed)

        return fe_eth_file_data

    def create_virtual_port_lookup(self, csv_file_list):
        """
        Creates a lookup for virtual port using veth_port.csv file present in metrics file. The lookup is a dictionary
        with key as fe_port_id extracted from extra_details column in the file. Value is the port-name extracted from
        name column in the same file.

        Args:
            csv_file_list (list[]): List of CSV config files.

        Returns:
            A dictionary with key as the fe_port_id and value as port name.

        Exceptions:
            ValueError if file is not accessible while creating dictionary for lookup.

        Test:
            test_humanize_base.py::test_create_virtual_port_lookup
        """""
        port_id_name_lookup = dict()

        # Process every config file in the list.
        for file_name in csv_file_list:
            try:
                with open(file_name) as file_handle:
                    reader = csv.reader(file_handle)
                    data = namedtuple('FileData', next(reader))

                    for row in reader:
                        file_data = data(*row)

                        # Extract column value for extra details column from the config file.
                        col = file_data.extra_details

                        # Extract fe_port_id value in the column to use as key.

                        key = re.search(self.FE_PORT_ID_REGEX, col).group(1)
                        if key:
                            port_id_name_lookup[key] = file_data.name

            except IOError:
                print('Could not open file {}'.format(file_name))
                return dict()

        return port_id_name_lookup


class HumanizeFcPort(Humanize):
    # Name of file with fc port details.
    FC_PORT_FILENAME = 'fc_port.csv'

    fc_port_lookup_dict = dict()

    def __init__(self, input_directory=None):
        super(HumanizeFcPort, self).__init__(input_directory)

        # From input directory list create list for fc port files.
        fc_port_csv_file_list = self.create_file_list(input_directory, self.FC_PORT_FILENAME)

        # Create fc port lookup with name and port index.
        self.fc_port_lookup_dict = self.perform_nested_lookup(fc_port_csv_file_list, [COLUMN_HEADER_NAME,
                                                                                      COLUMN_HEADER_INDEX])

    def add_details(self, filename):
        """
        A list of records is created from fe fc port metrics file. This method adds port details such as name and port
        index using fe port id for lookup. Columns are added to equate user friendly sizes, rates converting bytes to
        kilobytes and bytes per second to megabytes per second respectively. Once these calculations are done the list
        is written to the file with the help of a CSV writer.

        Args:
            filename (str): Name of metrics file that is to update.

        Returns:
           fe_fc_records: Number of records written in metric file.

        Exception:
            ValueError if metric file to update is not found.
        """""
        if not self.fc_port_lookup_dict:
            return 0

        input_filename = get_full_path_for_file(self.PROCESSED_METRICS_OUTPUT_DIR, filename)

        # Read data from fe fc port metrics file
        try:
            fe_fc_file_data = self.read_file_data(input_filename)

        except IOError:
            # Return if file could not be processed.
            print("Could not process file {}".format(filename))
            return 0

        # Add port details such as port name and port index.
        file_data_with_details = self.add_name_and_port_details(fe_fc_file_data, [COLUMN_HEADER_NAME,
                                                                                  'FC_port'])

        # Read size in KiB, write size in KiB and IO size in KiB is calculated for each record.
        data_with_average_details = self.add_average_size_details(file_data_with_details)

        # For each entry in metrics file total bandwidth, read bandwidth and write bandwidth is calculated in MiBPS.
        data_with_bandwidth_details = self.add_bandwidth_details(data_with_average_details)

        # For each entry in metrics file calculate unaligned read, write and total bandwidth in MiBPS.
        data_with_unaligned_details = self.add_unaligned_details(data_with_bandwidth_details)

        # Once the records are updated it is written back to metrics file.
        fe_fc_records = self.write_to_file(filename, data_with_unaligned_details)

        print('Records written in {} are {}'.format(filename, fe_fc_records))

        return fe_fc_records

    def add_name_and_port_details(self, fe_fc_file_data, header_list):
        """
        Adds port name and index details for fe fc port metrics file using fe port id for lookup.

        Args:
            fe_fc_file_data (list): List of file data.
            header_list (list): List of headers to append.

        Returns:
            A list of file data with name and port details.

        Test:
            test_humanize_base.py::test_add_name_and_port_details.
        """""
        file_data = namedtuple('FileData', fe_fc_file_data[0])

        for record in fe_fc_file_data[1:]:
            data = file_data(*record)

            name = get_value_from_key(data.fe_port_id, self.fc_port_lookup_dict, COLUMN_HEADER_NAME)
            record.append(name)

            fc_port = get_value_from_key(data.fe_port_id, self.fc_port_lookup_dict, COLUMN_HEADER_INDEX)
            record.append(fc_port)

        fe_fc_file_data[0].extend(header_list)

        return fe_fc_file_data

    def add_average_size_details(self, fe_fc_file_data):
        """
        Adds user friendly sizes from bytes to kilobytes. Equates average read, write and average io size to Kilobytes.

        Args:
            fe_fc_file_data (list): List of records in metrics file.

        Returns:
            A list of file data with average size details calculated for each record.

        Test:
            test_humanize_base.py::test_add_average_size_details

        """""
        file_data = namedtuple('FileData', fe_fc_file_data[0])

        for record in fe_fc_file_data[1:]:
            data = file_data(*record)

            read_size_kib = self.convert_to_kib(data.avg_read_size, 2)
            record.append(read_size_kib)

            write_size_kib = self.convert_to_kib(data.avg_write_size, 2)
            record.append(write_size_kib)

            io_size_kib = self.convert_to_kib(data.avg_io_size, 2)
            record.append(io_size_kib)

        fe_fc_file_data[0].extend(['avg_read_size_KiB', 'avg_write_size_KiB', 'average_io_size_KiB'])

        return fe_fc_file_data

    def add_bandwidth_details(self, fe_fc_file_data):
        """
        Adds bandwidth details from the fe fc metrics file. User friendly rates are added converting bytes per second
        to megabytes per second. Read, write and total bandwidth is equated to user friendly values.

        Args:
            fe_fc_file_data (list): List of records in metrics file.

        Returns:
            A list of file data with bandwidth details.

        Test:
            test_humanize_base.py::test_add_bandwidth_details
        """""
        file_data = namedtuple('FileData', fe_fc_file_data[0])

        for record in fe_fc_file_data[1:]:
            data = file_data(*record)

            read_bandwidth_mibps = self.convert_to_mib(data.read_bandwidth, 2)
            record.append(read_bandwidth_mibps)

            write_bandwidth_mibps = self.convert_to_mib(data.write_bandwidth, 2)
            record.append(write_bandwidth_mibps)

            total_bandwidth_mibps = self.convert_to_mib(data.total_bandwidth, 2)
            record.append(total_bandwidth_mibps)

        fe_fc_file_data[0].extend(['read_bandwidth_MiBPS', 'write_bandwidth_MiBPS', 'total_bandwidth_MiBPS'])

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
