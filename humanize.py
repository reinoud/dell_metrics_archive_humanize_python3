#!/usr/bin/env python
# -*- coding: utf-8 -*-
###########################################################################
# Copyright (C) Dell EMC 2020-2022
# All rights reserved.
# Licensed material -- property of Dell EMC
#
# humanize.py
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

import os
import sys

from humanize_appliance import HumanizeAppliance
from humanize_cache import HumanizeCache
from humanize_filesystem import HumanizeFileSystem
from humanize_hardware import HumanizeHardware
from humanize_node import HumanizeNode
from humanize_performance import HumanizeFiveSecMetrics
from humanize_ports import HumanizeEthPort, HumanizeFcPort
from humanize_volumes import HumanizeVolumes
from stitch import parse_args

# Name of the output directory for stitch.py.
PROCESSED_METRICS_OUTPUT_DIR = 'processed-metrics'

# Name of the output directory for humanize.
OUTPUT_DIR = 'output'

# These files have columns added to make them more human readable.
PERFORMANCE_METRICS_BY_FILE_SYSTEM = 'performance_metrics_by_file_system.csv'
PERFORMANCE_METRICS_BY_VOLUME = 'performance_metrics_by_volume.csv'
PERFORMANCE_METRICS_BY_CACHE_BY_NODE = 'performance_metrics_by_cache_by_node.csv'
PERFORMANCE_METRICS_BY_APPLIANCE = 'performance_metrics_by_appliance.csv'
PERFORMANCE_METRICS_BY_FEE_NODE = 'performance_metrics_by_fe_eth_node.csv'
PERFORMANCE_METRICS_BY_FEE_PORT = 'performance_metrics_by_fe_eth_port.csv'
PERFORMANCE_METRICS_BY_FE_FC_PORT = 'performance_metrics_by_fe_fc_port.csv'
PERFORMANCE_METRICS_BY_FE_FC_NODE = 'performance_metrics_by_fe_fc_node.csv'
PERFORMANCE_METRICS_BY_NODE = 'performance_metrics_by_node.csv'
PERFORMANCE_METRICS_BY_DRIVE_BY_NODE = 'performance_metrics_by_drive_by_node.csv'
PERFORMANCE_METRICS_BY_DRIVE_BY_APPLIANCE = 'performance_metrics_by_drive_by_appliance.csv'


def make_human_readable(input_directory_name_list=None):
    """
    This is where the processing of all metrics file begins. It searches the current directory for collection
    of subdirectories. In these directories it looks for configuration files which is used for creating a lookup of
    user friendly object names. It also processes "processed-metrics" directory to add additional columns in metric CSV
    files. It then re-writes these metrics file in an output directory.

    Args:
        input_directory_name_list ([str]): The directory to scan for configuration files to create a lookup.
    """""

    try:
        # Make sure there exists processed-metrics directory. This directory contains stitched metrics file which are
        # required for further processing.
        if not is_processed_metrics_directory_present():
            sys.exit(-1)

        # Make sure that the output directory either doesn't exist or if it does, it is empty.
        create_output_directory(OUTPUT_DIR)

        # Handle the default input directory list.
        if input_directory_name_list is None:
            input_directory_name_list = [os.getcwd()]

    except OSError as err:
        sys.stderr.write('Error while processing '.format(str(err)))
        sys.exit(-1)

    # Process file system metrics file.
    file_system = HumanizeFileSystem(input_directory_name_list)
    file_system.add_details(PERFORMANCE_METRICS_BY_FILE_SYSTEM)

    # Process volume metrics file
    volume_metrics = HumanizeVolumes(input_directory_name_list)
    volume_metrics.add_details(PERFORMANCE_METRICS_BY_VOLUME)

    # Process cache by node metrics file.
    cache_metrics = HumanizeCache(input_directory_name_list)
    cache_metrics.add_details(PERFORMANCE_METRICS_BY_CACHE_BY_NODE)

    # Process appliance metrics file.
    appliance_metrics = HumanizeAppliance(input_directory_name_list)
    appliance_metrics.add_details(PERFORMANCE_METRICS_BY_APPLIANCE)

    # Process node metrics files.
    node_metrics = HumanizeNode(input_directory_name_list)
    node_metrics.add_details(PERFORMANCE_METRICS_BY_FEE_NODE)
    node_metrics.add_fe_fc_node_details(PERFORMANCE_METRICS_BY_FE_FC_NODE)
    node_metrics.add_metrics_node_details(PERFORMANCE_METRICS_BY_NODE)

    # Process eth port files
    eth_port_metrics = HumanizeEthPort(input_directory_name_list)
    eth_port_metrics.add_details(PERFORMANCE_METRICS_BY_FEE_PORT)

    # Process fe fc port metrics file.
    fc_port_metrics = HumanizeFcPort(input_directory_name_list)
    fc_port_metrics.add_details(PERFORMANCE_METRICS_BY_FE_FC_PORT)

    # Process drive by node metrics file.
    hardware_metrics = HumanizeHardware(input_directory_name_list)
    hardware_metrics.add_details(PERFORMANCE_METRICS_BY_DRIVE_BY_NODE)
    hardware_metrics.add_metrics_by_drive_by_appliance_details(PERFORMANCE_METRICS_BY_DRIVE_BY_APPLIANCE)

    # Process 5 second performance metric files.
    five_sec_metrics = HumanizeFiveSecMetrics(input_directory_name_list)
    five_sec_metrics.add_details("")


def is_processed_metrics_directory_present():
    """
    Checks if processed-metrics directory is present. If not then it informs the user to execute stitch.py so
    processed-metrics directory is created.

    Return:
        Boolean: True if the path exists, false otherwise.

    """""
    if not os.path.exists(PROCESSED_METRICS_OUTPUT_DIR):
        sys.stderr.write('Execute stitch.py before processing metrics folder')
        return False
    return True


def create_output_directory(directory_name):
    """
    Create the specified directory, and optionally delete it if it already exists.

    Args:
        directory_name (str): The name of the directory to create, where output files get written.

    Raises OSError

    Test:
        test_humanize_base.py::test_okay_to_create_use_directory
    """""
    # Create a directory into which to write the results.
    try:
        os.mkdir(directory_name)

    except OSError as err:
        sys.stderr.write(
            'Cannot create directory: {}, please delete the directory if you need new results.\n'.format(str(err)))
        raise


def main(cmd_args):
    """
    Main processing. This is called with an array of arguments.

    Args:
        cmd_args ([str]): The command line arguments minus the program name.
    """""
    # Get the command line arguments and process them.
    args = parse_args(cmd_args)

    input_dir_list = args.input_dir if args.input_dir else None

    make_human_readable(input_dir_list)


if __name__ == '__main__':
    main(sys.argv[1:])
