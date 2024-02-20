#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###########################################################################
# Copyright (C) Dell EMC 2021-2022
# All rights reserved.
# Licensed material -- property of Dell EMC
#
# humanize_performance.py
#
###########################################################################
from __future__ import print_function
import operator
from collections import namedtuple
from datetime import datetime, timedelta
from humanize_base import Humanize, get_full_path_for_file
from humanize_hardware import append_column_value

PERFORMANCE_COUNTER_APPLIANCE_5_SEC = 'performance_counters_by_appliance_five_secs.csv'
PERFORMANCE_COUNTER_CLUSTER_5_SEC = 'performance_counters_by_cluster_five_secs.csv'
PERFORMANCE_COUNTER_HOST_5_SEC = 'performance_counters_by_host_five_secs.csv'
PERFORMANCE_COUNTER_HOST_GROUP_5_SEC = 'performance_counters_by_hg_five_secs.csv'
PERFORMANCE_COUNTER_INITIATOR_5_SEC = 'performance_counters_by_initiator_five_secs.csv'
PERFORMANCE_COUNTER_IP_PORT_5_SEC = 'performance_counters_by_ip_port_iscsi_five_secs.csv'
PERFORMANCE_COUNTER_NODE_5_SEC = 'performance_counters_by_node_five_secs.csv'
PERFORMANCE_COUNTER_ETH_NODE_5_SEC = 'performance_counters_by_fe_eth_node_five_secs.csv'
PERFORMANCE_COUNTER_FC_NODE_5_SEC = 'performance_counters_by_fe_fc_node_five_secs.csv'
PERFORMANCE_COUNTER_ETH_PORT_5_SEC = 'performance_counters_by_fe_eth_port_five_secs.csv'
PERFORMANCE_COUNTER_FC_PORT_5_SEC = 'performance_counters_by_fe_fc_port_five_secs.csv'
PERFORMANCE_COUNTER_VOL_5_SEC = 'performance_counters_by_volume_five_secs.csv'
PERFORMANCE_COUNTER_VG_5_SEC = 'performance_counters_by_vg_five_secs.csv'
PERFORMANCE_COUNTER_VM_5_SEC = 'performance_counters_by_vm_five_secs.csv'

# These counters do not have any rate conversions to perform
PERFORMANCE_COUNTER_CACHE_5_SEC = 'performance_counters_by_cache_by_node_five_secs.csv'
PERFORMANCE_COUNTER_DRIVE_5_SEC = 'performance_counters_by_drive_by_node_five_secs.csv'

# These are the SDNAS performance counters:
PERFORMANCE_COUNTER_NFS_5_SEC = 'performance_counters_nfs_by_node_five_secs.csv'
PERFORMANCE_COUNTER_SMB_5_SEC = 'performance_counters_smb_by_node_five_secs.csv'


class HumanizeFiveSecMetrics(Humanize):

    lookup_dict = dict()
    additional_cols = list()

    def __init__(self, input_directory=None):
        super(HumanizeFiveSecMetrics, self).__init__(input_directory)

        # Create a list of tuples consisting of (id, function name, column header, column name, rate name)
        # These will be used as follow:
        # Call the function and set the column_header = (column_name(t1)-column_name(t0))/(rate_name(t1)-rate_name(t0))
        # Those with a column name and rate name of None are special functions that require different calculations.
        self.additional_cols = \
            [(1, self.calculate_avg_rate, 'avg_read_latency', 'read_latency', 'read_ios'),
             (2, self.calculate_avg_rate, 'avg_write_latency', 'write_latency', 'write_ios'),
             (3, self.calculate_avg_latency, 'avg_latency', None, None),
             (4, self.calculate_avg_rate, 'avg_read_size', 'read_bytes', 'read_ios'),
             (5, self.calculate_avg_rate, 'avg_write_size', 'write_bytes', 'write_ios'),
             (6, self.calculate_avg_io_size, 'avg_io_size', None, None),
             (7, self.calculate_avg_rate, 'read_iops', 'read_ios', 'timestamp'),
             (8, self.calculate_avg_rate, 'write_iops', 'write_ios', 'timestamp'),
             (9, self.calculate_total_iops, 'total_iops', None, None),
             (10, self.calculate_avg_rate, 'read_bandwidth', 'read_bytes', 'timestamp'),
             (11, self.calculate_avg_rate, 'write_bandwidth', 'write_bytes', 'timestamp'),
             (12, self.calculate_total_bandwidth, 'total_bandwidth', None, None),
             (13, self.calculate_io_workload_util, 'io_workload_cpu_utilization', None, None),
             (14, self.calculate_avg_rate, 'dumped_frames_ps', 'dumped_frames', 'timestamp'),
             (15, self.calculate_avg_rate, 'loss_of_signal_count_ps', 'loss_of_signal_count', 'timestamp'),
             (16, self.calculate_avg_rate, 'invalid_crc_count_ps', 'invalid_crc_count', 'timestamp'),
             (17, self.calculate_avg_rate, 'loss_of_sync_count_ps', 'loss_of_sync_count', 'timestamp'),
             (18, self.calculate_avg_rate, 'invalid_tx_word_count_ps', 'invalid_tx_word_count', 'timestamp'),
             (19, self.calculate_avg_rate, 'prim_seq_prot_err_count_ps', 'prim_seq_prot_err_count', 'timestamp'),
             (20, self.calculate_avg_rate, 'link_failure_count_ps', 'link_failure_count', 'timestamp'),
             (21, self.calculate_avg_rate, 'pkt_rx_ps', 'pkt_rx', 'timestamp'),
             (22, self.calculate_avg_rate, 'pkt_tx_ps', 'pkt_tx', 'timestamp'),
             (23, self.calculate_avg_rate, 'bytes_tx_ps', 'bytes_tx', 'timestamp'),
             (24, self.calculate_avg_rate, 'bytes_rx_ps', 'bytes_rx', 'timestamp'),
             (25, self.calculate_avg_rate, 'pkt_rx_no_buffer_error_ps', 'pkt_rx_no_buffer_error', 'timestamp'),
             (26, self.calculate_avg_rate, 'pkt_rx_crc_error_ps', 'pkt_rx_crc_error', 'timestamp'),
             (27, self.calculate_avg_rate, 'pkt_tx_error_ps', 'pkt_tx_error', 'timestamp')]

        # Create a list of tuples consisting of metric files to convert to rates. Each tuple contains:
        # 1. The metric filename,
        # 2. A tuple of header columns used for sorting (example 1: cluster_id, timestamp)
        #                                               (example 2: appliance_id, cache_type, timestamp).
        # 3. A function name that will convert the csv values from all string values to the appropriate data types.
        # 4. A list of function id's from self.additional_cols to call for this particular metric file.
        self.metric_files = [
            (PERFORMANCE_COUNTER_APPLIANCE_5_SEC, ('appliance_id', 'timestamp'), self.appliance_record,
             list(range(1, 14, 1))),
            (PERFORMANCE_COUNTER_CLUSTER_5_SEC, ('cluster_id', 'timestamp'), self.cluster_record,
             list(range(1, 14, 1))),
            (PERFORMANCE_COUNTER_HOST_5_SEC, ('host_id', 'timestamp'), self.host_record,
             list(range(1, 7, 1))),
            (PERFORMANCE_COUNTER_HOST_GROUP_5_SEC, ('hg_id', 'timestamp'), self.host_group_record,
             list(range(1, 7, 1))),
            (PERFORMANCE_COUNTER_INITIATOR_5_SEC, ('initiator_id', 'timestamp'), self.initiator_record,
             list(range(1, 7, 1))),
            (PERFORMANCE_COUNTER_IP_PORT_5_SEC, ('ip_port_id', 'timestamp'), self.ip_port_record,
             list(range(1, 7, 1))),
            (PERFORMANCE_COUNTER_NODE_5_SEC, ('node_id', 'timestamp'), self.node_record,
             list(range(1, 14, 1))),
            (PERFORMANCE_COUNTER_FC_NODE_5_SEC, ('node_id', 'timestamp'), self.fe_fc_node_record,
             list(range(1, 13, 1)) + list(range(14, 21, 1))),
            (PERFORMANCE_COUNTER_FC_PORT_5_SEC, ('fe_port_id', 'timestamp'), self.fe_fc_port_record,
             list(range(1, 13, 1)) + list(range(14, 21, 1))),
            (PERFORMANCE_COUNTER_VOL_5_SEC, ('volume_id', 'timestamp'), self.volume_record,
             list(range(1, 7, 1))),
            (PERFORMANCE_COUNTER_VG_5_SEC, ('vg_id', 'timestamp'), self.vg_record,
             list(range(1, 7, 1))),
            (PERFORMANCE_COUNTER_VM_5_SEC, ('vm_id', 'timestamp'), self.vm_record,
             list(range(1, 7, 1))),
            (PERFORMANCE_COUNTER_ETH_NODE_5_SEC, ('node_id', 'timestamp'), self.fe_eth_node_record,
             list(range(21, 28, 1))),
            (PERFORMANCE_COUNTER_ETH_PORT_5_SEC, ('fe_port_id', 'timestamp'), self.fe_eth_port_record,
             list(range(21, 28, 1))),
            (PERFORMANCE_COUNTER_NFS_5_SEC, ('node_id', 'appliance_id', 'timestamp'), self.sdnas_nfs_record,
             list(range(1, 13, 1))),
            (PERFORMANCE_COUNTER_SMB_5_SEC, ('node_id', 'appliance_id', 'timestamp'), self.sdnas_smb_record,
             list(range(1, 13, 1)))
        ]

    def add_details(self, filename):
        """
        Take a list of metric files and convert them to rate values.

        Args:
            filename (str): Name of metrics file that is to update (ignored here)

        Exception:
            ValueError if file not found.
        """""
        del filename
        input_file = 0
        sort_criteria = 1
        translation_func = 2
        funcs_to_call = 3

        for records_to_convert in self.metric_files:
            self.convert_metrics_to_rates(records_to_convert[input_file],
                                          list(records_to_convert[sort_criteria]),
                                          records_to_convert[translation_func], records_to_convert[funcs_to_call])

    def convert_metrics_to_rates(self, filename, sort_criteria, translate_func, valid_func_list):
        """
        Adds additional columns to performance counter files by converting metric values to rate values.
        The data is first sorted by id and timestamp.

        Args:
            filename (str): Name of the performance counter file to be updated.
            sort_criteria ([int]): A list of indices to sort the csv table upon.
            translate_func: Function to convert the csv elements back to their appropriate datatype values.
            valid_func_list ([int]): A list of indices that reference the functions in additional_cols to invoke
               for the specific metric file.

        Returns:
            records_written: Records written in metric file system file.

        """""
        input_filename = get_full_path_for_file(self.PROCESSED_METRICS_OUTPUT_DIR, filename)

        # A list of records from cache by node metrics file
        try:
            five_sec_file_data = self.read_file_data(input_filename)

        except IOError:
            # Return if file could not be processed.
            print("Could not process file {}".format(filename))
            return 0

        # Need to ensure adjacent rows are same id, and then successive sample times.
        try:
            # Convert the column header names as the sort criteria into column index values.
            sort_indices = map(lambda x: five_sec_file_data[0].index(x), sort_criteria)

            # Convert the record of string values back to one with appropriate data type values.
            five_sec_file_data[1:] = [translate_func(*row) for row in five_sec_file_data[1:]]
            five_sec_file_data[1:] = sorted(five_sec_file_data[1:], key=operator.itemgetter(*sort_indices))

        except (IndexError, ValueError) as err:
            # Return if the index to sort upon is not valid
            print("The file {} could not be processed due to {}".format(filename, err))
            return 0

        column_id = 0
        function_name = 1
        column_to_add = 2
        column_numerator = 3
        column_denominator = 4
        for column in self.additional_cols:
            # If the additional column is in the valid func list, then perform the metrics to rate calculations.
            if column[column_id] in valid_func_list:
                five_sec_file_data = self.add_calculated_value(five_sec_file_data,
                                                               column[function_name], column[column_to_add],
                                                               column[column_numerator], column[column_denominator],
                                                               sort_indices)

        rate_records_written = self.write_to_file(filename, five_sec_file_data)

        print('Records written in {} are {}'.format(filename, rate_records_written))
        return rate_records_written

    @staticmethod
    def add_calculated_value(metric_file_data, func_name, col_to_add, col_numerator, col_denominator, compare_indices):
        """
        Adds value calculated from the function.

        Args:
         metric_file_data (list[]): Records present in metrics file.
         func_name (func): Function name to calculate field.
         col_to_add (str): Name of the column to add to the table
         col_numerator (str): Column name of the numerator to be used in the rate calculation
         col_denominator (str): Column name of the denominator to be used in the rate calculation
         compare_indices (list[]): indices of the record id label to use for comparison purposes.

        Returns:
            A list with values appended in the end of each record.
        """""

        # Create a namedtuple object using header row from file data.
        header = namedtuple('Metrics', metric_file_data[0])

        # Comparison between two consecutive records is required to calculate required value.
        # Calculation starts from the second record in the file as first record is a header row hence it is ignored.
        calculated_value = [func_name(time0, time1, header, compare_indices, col_numerator, col_denominator)
                            for time0, time1 in zip(metric_file_data[1:], metric_file_data[2:])]
        # calculated_value = list()
        # for time0, time1 in zip(metric_file_data[1:], metric_file_data[2:]):
        #     t0 = header(*time0)
        #     t1 = header(*time1)
        #     if t0.timestamp != t1.timestamp and t0[column_index_id] == t1[column_index_id]:
        #         calculated_value.append(func_name(time0, time1, header, column_index_id))

        # Value header is appended for each row and appended at the end. Header row is updated as well for total MB.
        data_with_calculated_value = append_column_value(metric_file_data, calculated_value, col_to_add)

        return data_with_calculated_value

    @staticmethod
    def calculate_avg_latency(time0, time1, header, compare_indices, unused1, unused2):
        """
        Calculate the average latency between two consecutive records.

        Args:
            time0 (list[]): Previous record
            time1 (list[]): Current Record
            header (namedtuple): Header row to create namedtuple object.
            compare_indices (list[]): indices of the record id label to use for comparison purposes.
            unused1 (str): Not used in this method - is a place holder for col_header in calculate_avg_rate
            unused2 (str): Not used in this method - is a place holder for rate_header in calculate_avg_rate

        Returns:
            average latency value.
        """""
        del unused1
        del unused2

        t0 = header(*time0)
        t1 = header(*time1)

        # Perform calculation only if two consecutive records have different timestamp values and the same id value.
        # Return 0 if otherwise.
        # The compare_indices contains one or more id values and the last is the index to the timestamp.
        same_id = map(lambda index: t0[index] == t1[index], list(compare_indices)[:-1])
        if t0.timestamp != t1.timestamp and all(same_id):
            try:
                read_ios_diff = t1.read_ios - t0.read_ios
                write_ios_diff = t1.write_ios - t0.write_ios
                ios_diff = read_ios_diff + write_ios_diff
                if ios_diff != 0:
                    return ((t1.read_latency - t0.read_latency) + (t1.write_latency - t0.write_latency)) / ios_diff
            except AttributeError:
                pass

        return 0.0

    @staticmethod
    def calculate_avg_io_size(time0, time1, header, compare_indices, unused1, unused2):
        """
        Calculate the average io size between two consecutive records.

        Args:
            time0 (list[]): Previous record
            time1 (list[]): Current Record
            header (namedtuple): Header row to create namedtuple object.
            compare_indices (list[]): indices of the record id label to use for comparison purposes.
            unused1 (str): Not used in this method - is a place holder for col_header in calculate_avg_rate
            unused2 (str): Not used in this method - is a place holder for rate_header in calculate_avg_rate

        Returns:
            average io size value.
        """""
        del unused1
        del unused2

        t0 = header(*time0)
        t1 = header(*time1)

        # Perform calculation only if two consecutive records have different timestamp values and the same id value.
        # Return 0 if otherwise.
        # The compare_indices contains one or more id values and the last is the index to the timestamp.
        same_id = map(lambda index: t0[index] == t1[index], list(compare_indices)[:-1])
        if t0.timestamp != t1.timestamp and all(same_id):
            try:
                read_ios_diff = t1.read_ios - t0.read_ios
                write_ios_diff = t1.write_ios - t0.write_ios
                ios_diff = read_ios_diff + write_ios_diff
                if ios_diff != 0:
                    return ((t1.read_bytes - t0.read_bytes) + (t1.write_bytes - t0.write_bytes)) / ios_diff
            except AttributeError:
                pass

        return 0.0

    @staticmethod
    def calculate_total_iops(time0, time1, header, compare_indices, unused1, unused2):
        """
        Calculate the total iops between two consecutive records.

        Args:
            time0 (list[]): Previous record
            time1 (list[]): Current Record
            header (namedtuple): Header row to create namedtuple object.
            compare_indices (list[]): indices of the record id label to use for comparison purposes.
            unused1 (str): Not used in this method - is a place holder for col_header in calculate_avg_rate
            unused2 (str): Not used in this method - is a place holder for rate_header in calculate_avg_rate

        Returns:
            total iops value.
        """""
        del unused1
        del unused2

        t0 = header(*time0)
        t1 = header(*time1)

        # Perform calculation only if two consecutive records have different timestamp values and the same id value.
        # Return 0 if otherwise.
        # The compare_indices contains one or more id values and the last is the index to the timestamp.
        same_id = map(lambda index: t0[index] == t1[index], list(compare_indices)[:-1])
        if t0.timestamp != t1.timestamp and all(same_id):
            try:
                time_diff = (t1.timestamp - t0.timestamp).total_seconds()
                if time_diff != 0:
                    return ((t1.read_ios - t0.read_ios) + (t1.write_ios - t0.write_ios)) / time_diff
            except AttributeError:
                pass

        return 0.0

    @staticmethod
    def calculate_total_bandwidth(time0, time1, header, compare_indices, unused1, unused2):
        """
        Calculate the total bandwidth between two consecutive records.

        Args:
            time0 (list[]): Previous record
            time1 (list[]): Current Record
            header (namedtuple): Header row to create namedtuple object.
            compare_indices (list[]): indices of the record id label to use for comparison purposes.
            unused1 (str): Not used in this method - is a place holder for col_header in calculate_avg_rate
            unused2 (str): Not used in this method - is a place holder for rate_header in calculate_avg_rate

        Returns:
            total bandwidth value or 0 if records don't have matching id values.
        """""
        del unused1
        del unused2

        t0 = header(*time0)
        t1 = header(*time1)

        # Perform calculation only if two consecutive records have different timestamp values and the same id value.
        # The compare_indices contains one or more id values and the last is the index to the timestamp.
        same_id = map(lambda index: t0[index] == t1[index], list(compare_indices)[:-1])
        if t0.timestamp != t1.timestamp and all(same_id):
            try:
                time_diff = (t1.timestamp - t0.timestamp).total_seconds()
                if time_diff != 0:
                    return ((t1.read_bytes - t0.read_bytes) + (t1.write_bytes - t0.write_bytes)) / time_diff
            except AttributeError:
                pass
        return 0.0

    @staticmethod
    def calculate_io_workload_util(time0, time1, header, compare_indices, unused1, unused2):
        """
        Calculate the io workload CPU utilization between two consecutive records.

        Args:
            time0 (list[]): Previous record
            time1 (list[]): Current Record
            header (namedtuple): Header row to create namedtuple object.
            compare_indices (list[]): indices of the record id label to use for comparison purposes.
            unused1 (str): Not used in this method - is a place holder for col_header in calculate_avg_rate
            unused2 (str): Not used in this method - is a place holder for reate_header in calculate_avg_rate

        Returns:
            io workload CPU utilization value.
        """""
        del unused1
        del unused2

        t0 = header(*time0)
        t1 = header(*time1)

        # Perform calculation only if two consecutive records have different timestamp values and the same id value.
        # Return 0 if otherwise.
        # The compare_indices contains one or more id values and the last is the index to the timestamp.
        same_id = map(lambda index: t0[index] == t1[index], list(compare_indices)[:-1])
        if t0.timestamp != t1.timestamp and all(same_id):
            try:
                total_ticks = t1.total_ticks - t0.total_ticks
                if total_ticks != 0:
                    return (total_ticks - (t1.idle_ticks - t0.idle_ticks)) / total_ticks
            except AttributeError:
                pass
        return 0.0

    @staticmethod
    def calculate_avg_rate(time0, time1, header, compare_indices, col_header, rate_header):
        """
        Formulas for the calculations can be found at:
        https://confluence.cec.lab.emc.com/pages/viewpage.action?spaceKey=CYCLONE&title=TRIF-592+Counters+to+rates

        Calculate the average values between two timed records (time0, time1) using the specified column attribute.
            Example1: (read_latency(T1) - read_latency(T0)) / (read_ios(T1) - read_ios(T0))
            In this case:
                    col_header = read_latency
                    rate_header = read_ios

            Example2: (read_bytes(T1) - read_bytes(T0)) / (T1 - T0)
            In this case:
                    col_header = read_bytes
                    rate_header = timestamp
        Args:
            time0 (list[]): Previous record
            time1 (list[]): Current Record
            header (namedtuple): Header row to create namedtuple object.
            compare_indices (list[]): indices of the record id label to use for comparison purposes.
            col_header (str): name of the column header to use to perform the rate calculation.
            rate_header (str): name of the column header used as the average value in the rate calculation.

        Returns:
            total bandwidth value or 0 if records don't have matching id values.
        """""
        t0 = header(*time0)
        t1 = header(*time1)

        # Perform calculation only if two consecutive records have different timestamp values and the same id value.
        # The compare_indices contains one or more id values and the last is the index to the timestamp.
        same_id = map(lambda index: t0[index] == t1[index], list(compare_indices)[:-1])
        if t0.timestamp != t1.timestamp and all(same_id):
            try:
                rate_diff = getattr(t1, rate_header) - getattr(t0, rate_header)

                # If the rate header is a timestamp, convert the difference into seconds.
                if rate_header == 'timestamp':
                    rate_diff = rate_diff.total_seconds()

                if rate_diff != 0:
                    return (getattr(t1, col_header) - getattr(t0, col_header)) / rate_diff

            except AttributeError:
                pass
        return 0.0

    @staticmethod
    def str_to_datetime(str_time):
        """
        Convert a string representation of the datetime to an actual datetime object

        Args:
            str_time (str): A string representation of time including timezone information
                Example: str_time = '2020-11-01 20:44:15+00:00'

        Returns:
            datetime object
        """""
        return datetime.strptime(str_time[:19], '%Y-%m-%d %H:%M:%S') + \
            timedelta(hours=int(str_time[20:22]), minutes=int(str_time[23:])) * (-1 if str_time[19] == '+' else 1)

    def appliance_record(self, appliance_id, timestamp, read_latency, write_latency, read_ios, write_ios,
                         read_bytes, write_bytes, mirror_write_ios, mirror_write_latency, mirror_overhead_latency,
                         mirror_write_bytes, idle_ticks, total_ticks):
        """
        Convert the appliance record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str, str, str, str, str, str) ->
              (str, datetime, int, int, int, int, int, int, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [appliance_id, self.str_to_datetime(timestamp), int(read_latency), int(write_latency),
                int(read_ios), int(write_ios), int(read_bytes), int(write_bytes), int(mirror_write_ios),
                int(mirror_write_latency), int(mirror_overhead_latency), int(mirror_write_bytes),
                int(idle_ticks), int(total_ticks)]

    def cluster_record(self, timestamp, cluster_id, read_latency, write_latency, read_ios, write_ios,
                       read_bytes, write_bytes, mirror_write_ios, mirror_write_latency, mirror_overhead_latency,
                       mirror_write_bytes, idle_ticks, total_ticks):
        """
        Convert the cluster record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str, str, str, str, str, str) ->
              (datetime, str, int, int, int, int, int, int, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [self.str_to_datetime(timestamp), cluster_id, int(read_latency), int(write_latency),
                int(read_ios), int(write_ios), int(read_bytes), int(write_bytes), int(mirror_write_ios),
                int(mirror_write_latency), int(mirror_overhead_latency), int(mirror_write_bytes),
                int(idle_ticks), int(total_ticks)]

    def node_record(self, node_id, appliance_id, timestamp, read_latency, write_latency, read_ios, write_ios,
                    read_bytes, write_bytes, mirror_write_ios, mirror_write_latency, mirror_overhead_latency,
                    mirror_write_bytes, idle_ticks, total_ticks):
        """
        Convert the node record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str, str, str, str, str, str, str) ->
              (str, str, datetime, int, int, int, int, int, int, float, float, float, float, int, int)

        Return:
            list of values
        """""
        return [node_id, appliance_id, self.str_to_datetime(timestamp), int(read_latency), int(write_latency),
                int(read_ios), int(write_ios), int(read_bytes), int(write_bytes),  float(mirror_write_ios),
                float(mirror_write_latency), float(mirror_overhead_latency), float(mirror_write_bytes),
                int(idle_ticks), int(total_ticks)]

    def initiator_record(self, initiator_id, timestamp, read_latency, write_latency, read_ios, write_ios,
                         read_bytes, write_bytes):
        """
        Convert the initiator record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str) -> (str, datetime, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [initiator_id, self.str_to_datetime(timestamp), int(read_latency), int(write_latency),
                int(read_ios), int(write_ios), int(read_bytes), int(write_bytes)]

    def fe_fc_port_record(self, node_id, appliance_id, fe_port_id, timestamp, read_latency, write_latency,
                          read_ios, write_ios, read_bytes, write_bytes, total_logins, dumped_frames,
                          loss_of_signal_count, invalid_crc_count, loss_of_sync_count, invalid_tx_word_count,
                          prim_seq_prot_err_count, link_failure_count):
        """
        Convert the fe fc port record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str) ->
              (str, str, str, datetime, int, int, int, int, int, int, int, int, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [node_id, appliance_id, fe_port_id, self.str_to_datetime(timestamp), int(read_latency),
                int(write_latency), int(read_ios), int(write_ios), int(read_bytes), int(write_bytes),
                int(total_logins),
                int(dumped_frames), int(loss_of_signal_count), int(invalid_crc_count), int(loss_of_sync_count),
                int(invalid_tx_word_count), int(prim_seq_prot_err_count), int(link_failure_count)]

    def fe_fc_node_record(self, node_id, appliance_id, timestamp, read_latency, write_latency, read_ios, write_ios,
                          read_bytes, write_bytes, total_logins, dumped_frames, loss_of_signal_count,
                          invalid_crc_count, loss_of_sync_count, invalid_tx_word_count, prim_seq_prot_err_count,
                          link_failure_count):
        """
        Convert the fe fc node record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str) ->
              (str, str, datetime, int, int, int, int, int, int, int, int, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [node_id, appliance_id, self.str_to_datetime(timestamp), int(read_latency), int(write_latency),
                int(read_ios), int(write_ios), int(read_bytes), int(write_bytes), int(total_logins),
                int(dumped_frames), int(loss_of_signal_count), int(invalid_crc_count), int(loss_of_sync_count),
                int(invalid_tx_word_count), int(prim_seq_prot_err_count), int(link_failure_count)]

    def fe_eth_port_record(self, fe_port_id, node_id, appliance_id, timestamp, pkt_rx, pkt_tx, bytes_tx, bytes_rx,
                           pkt_rx_no_buffer_error, pkt_rx_crc_error, pkt_tx_error):
        """
        Convert the fe eth port record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str, str, str) ->
              (str, str, str, datetime, int, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [fe_port_id, node_id, appliance_id, self.str_to_datetime(timestamp), int(pkt_rx), int(pkt_tx),
                int(bytes_tx), int(bytes_rx), int(pkt_rx_no_buffer_error), int(pkt_rx_crc_error), int(pkt_tx_error)]

    def fe_eth_node_record(self, node_id, appliance_id, timestamp, pkt_rx, pkt_tx, bytes_tx, bytes_rx,
                           pkt_rx_no_buffer_error, pkt_rx_crc_error, pkt_tx_error):
        """
        Convert the fe eth node record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str, str) ->
              (str, str, datetime, int, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [node_id, appliance_id, self.str_to_datetime(timestamp), int(pkt_rx), int(pkt_tx),
                int(bytes_tx), int(bytes_rx), int(pkt_rx_no_buffer_error), int(pkt_rx_crc_error), int(pkt_tx_error)]

    def drive_record(self, timestamp, appliance_id, node_id, drive_id, read_latency, write_latency,
                     read_ios, write_ios, read_bytes, write_bytes, queue_len, state):
        """
        Convert the drive record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str, str) ->
              (datetime, str, str, str, int, int, int, int, int, int, int, str)

        Return:
            list of values
        """""
        return [self.str_to_datetime(timestamp), appliance_id, node_id, drive_id, int(read_latency),
                int(write_latency), int(read_ios), int(write_ios), int(read_bytes), int(write_bytes),
                int(queue_len), state]

    def cache_record(self, timestamp, appliance_id, cache_type, node_id, pages_local, pages_peer, total_bytes,
                     dirty_pages_local, dirty_pages_peer, total_dirty_bytes, pages_being_held, flush_page_merges,
                     waits_for_pages, fast_page_lookups, access_lookups, cache_lookups, cache_fast_lookups,
                     flush_requests, pages_holds, page_holds_rolledback, pages_flushed, load_during_holds, flush_zeros,
                     flush_completes, forced_flushes, locked_retried, fua_locked_retried, free_pages,
                     cpydff_tail_wait_current, cpydff_tail_wait_highwtr, cpydff_num_of_timeouts,
                     cpydff_largest_tail_lsn, cpydff_num_of_flushes, suspended, suspend_calls, resume_calls,
                     unaligned_ios, drive_reads, page_overwrites, page_read_hits, page_read_misses,
                     page_read_invalid_found, page_write_invalid_found, page_writes_all, page_write_misses,
                     page_write_hits, page_write_hits_dirty, page_read_pure_hits, page_read_hit_locks,
                     page_read_log_loads, page_read_mapper_loads, page_scan_invalidates, ext_rec_scan_requeues,
                     ext_rec_scan_completed, ext_rec_aborted, max_ext_rec_scan_time, ext_page_scan_evaluated,
                     ontk_pgclns_tlmv, ontk_pgclns_cpblow, ontk_pgclns_ondmnd, ontk_pgclns_cachethtl,
                     offtk_pgclns_tlmv, offtk_pgclns_cpblow, offtk_pgclns_ondmnd, offtk_pgclns_cachethtl,
                     cpb_getbuf_zeroes, skipped_pgclns_bthnodes, clean_reschedules, ios_started, ios_completed,
                     ios_active, log_full_waits, cache_full_waits, cpydff_total_requests, cpydff_total_tail_waits,
                     peerpdb_failed_tryget_retry, hold_try_access_failure):
        """
        Convert the cache record values from string back to its appropriate data types.

        Return:
            list of values
        """""
        return [self.str_to_datetime(timestamp), appliance_id, cache_type, node_id, int(pages_local), int(pages_peer),
                int(total_bytes), int(dirty_pages_local), int(dirty_pages_peer), int(total_dirty_bytes),
                int(pages_being_held), int(flush_page_merges), int(waits_for_pages), int(fast_page_lookups),
                int(access_lookups), int(cache_lookups), int(cache_fast_lookups), int(flush_requests),
                int(pages_holds) if pages_holds else 0, int(page_holds_rolledback), int(pages_flushed),
                int(load_during_holds), int(flush_zeros), int(flush_completes), int(forced_flushes),
                int(locked_retried), int(fua_locked_retried), int(free_pages), int(cpydff_tail_wait_current),
                int(cpydff_tail_wait_highwtr), int(cpydff_num_of_timeouts), int(cpydff_largest_tail_lsn),
                int(cpydff_num_of_flushes), int(suspended), int(suspend_calls), int(resume_calls), int(unaligned_ios),
                int(drive_reads), int(page_overwrites), int(page_read_hits), int(page_read_misses),
                int(page_read_invalid_found), int(page_write_invalid_found), int(page_writes_all),
                int(page_write_misses), int(page_write_hits), int(page_write_hits_dirty), int(page_read_pure_hits),
                int(page_read_hit_locks), int(page_read_log_loads), int(page_read_mapper_loads),
                int(page_scan_invalidates), int(ext_rec_scan_requeues), int(ext_rec_scan_completed),
                int(ext_rec_aborted), int(max_ext_rec_scan_time), int(ext_page_scan_evaluated), int(ontk_pgclns_tlmv),
                int(ontk_pgclns_cpblow), int(ontk_pgclns_ondmnd), int(ontk_pgclns_cachethtl), int(offtk_pgclns_tlmv),
                int(offtk_pgclns_cpblow), int(offtk_pgclns_ondmnd), int(offtk_pgclns_cachethtl),
                int(cpb_getbuf_zeroes), int(skipped_pgclns_bthnodes), int(clean_reschedules), int(ios_started),
                int(ios_completed), int(ios_active), int(log_full_waits), int(cache_full_waits),
                int(cpydff_total_requests), int(cpydff_total_tail_waits), int(peerpdb_failed_tryget_retry),
                int(hold_try_access_failure)]

    def host_record(self, host_id, timestamp, read_latency, write_latency, read_ios, write_ios, read_bytes,
                    write_bytes):
        """
        Convert the host record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str) -> (str, datetime, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [host_id, self.str_to_datetime(timestamp), int(read_latency), int(write_latency),
                int(read_ios), int(write_ios), int(read_bytes), int(write_bytes)]

    def host_group_record(self, hg_id, timestamp, read_latency, write_latency, read_ios, write_ios, read_bytes,
                          write_bytes):
        """
        Convert the host group record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str) -> (str, datetime, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [hg_id, self.str_to_datetime(timestamp), int(read_latency), int(write_latency),
                int(read_ios), int(write_ios), int(read_bytes), int(write_bytes)]

    def volume_record(self, volume_id, timestamp, current_appliance_id, appliance_id, read_latency, write_latency,
                      read_ios, write_ios, read_bytes, write_bytes, mirror_write_ios, mirror_overhead_latency,
                      mirror_write_bytes):
        """
        Convert the volume record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str, str, str, str, str) ->
              (str, datetime, str, str, int, int, int, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [volume_id, self.str_to_datetime(timestamp), current_appliance_id, appliance_id, int(read_latency),
                int(write_latency), int(read_ios), int(write_ios), int(read_bytes), int(write_bytes),
                int(mirror_write_ios), int(mirror_overhead_latency), int(mirror_write_bytes)]

    def vg_record(self, vg_id, current_appliance_id, appliance_id, timestamp, read_latency, write_latency,
                  read_ios, write_ios, read_bytes, write_bytes):
        """
        Convert the volume group record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str, str) ->
              (str, str, str, datetime, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [vg_id, current_appliance_id, appliance_id, self.str_to_datetime(timestamp), int(read_latency),
                int(write_latency), int(read_ios), int(write_ios), int(read_bytes), int(write_bytes)]

    def vm_record(self, vm_id, timestamp, read_latency, write_latency, read_ios, write_ios, read_bytes, write_bytes):
        """
        Convert the virtual machine record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str) -> (str, datetime, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [vm_id, self.str_to_datetime(timestamp), int(read_latency), int(write_latency),
                int(read_ios), int(write_ios), int(read_bytes), int(write_bytes)]

    def ip_port_record(self, ip_port_id, appliance_id, timestamp, read_latency, write_latency, read_ios, write_ios,
                       read_bytes, write_bytes):
        """
        Convert the virtual machine record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str) -> (str, str, datetime, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [ip_port_id, appliance_id, self.str_to_datetime(timestamp), int(read_latency), int(write_latency),
                int(read_ios), int(write_ios), int(read_bytes), int(write_bytes)]

    def sdnas_nfs_record(self, node_id, appliance_id, timestamp, read_ios, write_ios, read_latency, write_latency,
                         read_bytes, write_bytes):
        """
        Convert the sdnas_nfs_by_node_five_sec record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str) -> (str, str, datetime, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [node_id, appliance_id, self.str_to_datetime(timestamp), int(read_ios), int(write_ios),
                int(read_latency), int(write_latency), int(read_bytes), int(write_bytes)]

    def sdnas_smb_record(self, node_id, appliance_id, timestamp, read_ios, write_ios, read_latency, write_latency,
                         read_bytes, write_bytes, total_calls, current_tcp_connections):
        """
        Convert the sdnas_nfs_by_node_five_sec record values from string back to its appropriate data types.

        type: (str, str, str, str, str, str, str, str, str, str, str) ->
              (str, str, datetime, int, int, int, int, int, int, int, int)

        Return:
            list of values
        """""
        return [node_id, appliance_id, self.str_to_datetime(timestamp), int(read_ios), int(write_ios),
                int(read_latency), int(write_latency), int(read_bytes), int(write_bytes), int(total_calls),
                int(current_tcp_connections)]
