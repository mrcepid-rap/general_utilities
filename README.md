# General Utilities

## Changelog

* v1.1.2
  * `download_dxfile_by_name` now additional accepts a `Path` and returns a `Path` instead of a string.
  * Implemented `MRCLogger` for all methods in `association_resources.py`
    * Minor changes to run_cmd() to facilitate this change (which had already implemented MRCLogger)
  * MRCLogger should now function properly outside the DNANexus environment

* v1.1.1
  * Fixed a bug in find_index() that wouldn't properly search outside the users current project

* v1.1.0
  * Adding an import_utils package that includes classes for import of data
  * Fixed a bug that caused MRCLogger to print multiple lines if the logger was built multiple times with the same class name

* v1.0.1
  * Added better documentation for MRCLogger
  * Fixed a bug in run_cmd() causing logs to be printed N times, where N is the number of times the class has been used by the applet 
  
* v1.0.0
  * Initial numbered release, previous commits and added functionality are included in the GitHub commits