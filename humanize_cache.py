#!/usr/bin/env python
# -*- coding: utf-8 -*-
###########################################################################
# Copyright (C) Dell EMC 2020-2021
# All rights reserved.
# Licensed material -- property of Dell EMC
#
# humanize_cache.py
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

import operator
from collections import namedtuple
from humanize_base import Humanize, get_full_path_for_file
from humanize_hardware import append_column_value


class HumanizeCache(Humanize):

    additional_cols = list()

    def __init__(self, input_directory=None):
        super(HumanizeCache, self).__init__(input_directory)
        # Create a list of tuples consisting of (function name, column header, column name)
        self.additional_cols = \
            [(self.calculate_total_mb, 'total_Mbytes', None),
             (self.calculate_dirty_pages_local_in_gb, 'dirty_pages_local_GB', None),
             (self.calculate_dirty_pages_peer_gb, 'dirty_page_peer_GB', None),
             (self.calculate_total_dirty_pages_mbps, 'total_dirty_Mbytes_ps', None),
             (self.calculate_pages_being_held_gb, 'pages_being_held_GB', None),
             (self.compare_and_calculate, 'flush_pages_merge_ps', 'flush_page_merges'),
             (self.compare_and_calculate, 'wait_for_pages_ps', 'waits_for_pages'),
             (self.compare_and_calculate, 'fast_page_lookups_ps', 'fast_page_lookups'),
             (self.compare_and_calculate, 'access_lookups_ps', 'access_lookups'),
             (self.compare_and_calculate, 'cache_lookups_ps', 'cache_lookups'),
             (self.compare_and_calculate, 'cache_fast_lookups_ps', 'cache_fast_lookups'),
             (self.compare_and_calculate, 'flush_requests_ps', 'flush_requests'),
             (self.compare_and_calculate, 'page_holds_rollbacked_ps', 'page_holds_rolledback'),
             (self.compare_and_calculate, 'pages_flushed_ps', 'pages_flushed'),
             (self.compare_and_calculate, 'load_during_holds_ps', 'load_during_holds'),
             (self.compare_and_calculate, 'flush_zeros_ps', 'flush_zeros'),
             (self.compare_and_calculate, 'flush_completes_ps', 'flush_completes'),
             (self.compare_and_calculate, 'unaligned_ios_ps', 'unaligned_ios'),
             (self.compare_and_calculate, 'free_pages_MB', 'free_pages'),
             (self.compare_and_calculate, 'drive_reads_ps', 'drive_reads'),
             (self.compare_and_calculate, 'page_read_hits_ps', 'page_read_hits'),
             (self.compare_and_calculate, 'page_read_misses_ps', 'page_read_misses'),
             (self.compare_and_calculate, 'pages_writes_all_ps', 'page_writes_all'),
             (self.compare_and_calculate, 'page_write_misses_ps', 'page_write_misses'),
             (self.compare_and_calculate, 'page_write_hits_ps', 'page_write_hits'),
             (self.compare_and_calculate, 'page_write_hits_dirty_ps', 'page_write_hits_dirty'),
             (self.compare_and_calculate, 'page_read_pure_hits_ps', 'page_read_pure_hits'),
             (self.compare_and_calculate, 'page_read_hits_locks_ps', 'page_read_hit_locks'),
             (self.compare_and_calculate, 'page_read_mapper_loads_ps', 'page_read_mapper_loads'),
             (self.compare_and_calculate, 'ios_started_ps', 'ios_started'),
             (self.compare_and_calculate, 'ios_completed_ps', 'ios_completed'),
             (self.compare_and_calculate, 'ios_active_ps', 'ios_active'),
             (self.compare_and_calculate, 'log_full_waits_ps', 'log_full_waits'),
             (self.compare_and_calculate, 'cache_full_waits_ps', 'cache_full_waits')
             ]

    def add_details(self, filename):
        """
        Adds additional columns in cache by node metrics file. The data in this file is first sorted in order of
        appliance, cache typed and then node. Cache utilization columns are added using page hits and page misses
        information. Formulas are applied to determine delta for page read-write details, lookup details etc.

        Args:
            filename (str): Name of metrics file that is to update.

        Returns:
            records_written: Records written in metric file system file.

        Exception:
            ValueError if file not found.
        """""
        file_name = get_full_path_for_file(self.PROCESSED_METRICS_OUTPUT_DIR, filename)

        # A list of records from cache by node metrics file
        try:
            cache_file_data = self.read_file_data(file_name)

        except IOError:
            # Return if file could not be processed.
            print("Could not process file {}".format(filename))
            return 0

        # Need to ensure adjacent rows are same appliance, same cache type,
        # same node and then successive sample times.
        cache_file_data[1:] = sorted(cache_file_data[1:], key=operator.itemgetter(1, 2, 3))

        for column in self.additional_cols:
            cache_file_data = self.add_calculated_value(cache_file_data, column[0], column[1], column[2])

        # Add cache utilization details for each record.
        data_with_cache_utilization = self.add_data_with_cache_utilization(cache_file_data)

        cache_records_written = self.write_to_file(filename, data_with_cache_utilization)

        print('Records written in {} are {}'.format(filename, cache_records_written))
        return cache_records_written

    @staticmethod
    def add_calculated_value(cache_file_data, func_name, column_header, column_name=None):
        """
        Adds value calculated from the function.

        Args:
         cache_file_data (list[]): Records present in cache by node metrics file.
         func_name (func): Function name to calculate field.
         column_header (str):
         column_name (str): Column Name for calculating.

        Returns:
            A list with values appended in the end of each record.
        """""

        # Create a namedtuple object using header row from file data.
        cache = namedtuple('Cache', cache_file_data[0])

        # Comparison between two consecutive records is required to calculate required value.
        # Calculation starts from the second record in the file as first record is a header row hence it is ignored.
        calculated_value = [func_name(prev, curr, cache, column_name) if column_name else func_name(prev, curr, cache)
                            for prev, curr in zip(cache_file_data[1:], cache_file_data[2:])]

        # Value header is appended for each row and appended at the end. Header row is updated as well for total MB.
        data_with_calculated_value = append_column_value(cache_file_data, calculated_value, column_header)

        return data_with_calculated_value

    def add_data_with_cache_utilization(self, cache_file_data):
        """
        Adds cache utilization to each record in cache file data. Write cache is calculated using page write hits and
        misses values. Read cache utilization is calculated using page read hits and misses. Header for read and write
        calculation is appended.

        Args:
            cache_file_data (list[]): Records present in cache by node metrics file.

        Returns:
            A list of records with cache_utilization calculated for each record.
        """""
        # Create a namedtuple object using header row from file data.
        cache = namedtuple('Cache', cache_file_data[0])

        for data in cache_file_data[1:]:
            cache_data = cache(*data)

            # Calculate write cache value and append to the record.
            write_cache = self.calculate_cache_utilization(float(cache_data.page_write_hits_ps),
                                                           float(cache_data.page_write_misses_ps))
            data.append(write_cache)

            # Calculate read cache value and append to the record
            read_cache = self.calculate_cache_utilization(float(cache_data.page_read_hits_ps),
                                                          float(cache_data.page_read_misses_ps))
            data.append(read_cache)

        # Append headers for read_cache and write cache utilization.
        cache_file_data[0].extend(['write_cache_utilization', 'read_cache_utilization'])

        return cache_file_data

    @staticmethod
    def calculate_cache_utilization(data_hits, data_misses):
        """
        Calculates cache utilization using data hit and data misses values.

        Args:
            data_hits (float): data hits value.
            data_misses (float): data miss value.

        Returns:
            cache utilization value.
        """""
        try:
            value = data_hits / (data_hits + data_misses)

        except ZeroDivisionError:
            return 0
        else:
            return round(100 * value, 2)
