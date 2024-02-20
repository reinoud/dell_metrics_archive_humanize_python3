# Overview

Dell Powerstore systems can export metrics_archives that contain internal metrics for up to several days.

To assist in further usage, the files contain a set of Python scripts. These scripts are in Python2, and fail under a more modern Python3 version.

This repository contains fixed scripts.

# differences

The differences are not very big:

  - change the header to use the python3 executable (changed from python)
  - convert range() functions to list(range()) statements (Python3 range functions returns an iterable)
  - convert map() functions to list(map()) statements (Python3 maps are not subscriptable)

Tags have been set to help see the differences:

```git diff Python2 Python3```

