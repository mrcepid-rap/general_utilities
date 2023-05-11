# General Utilities

## Changelog

* v1.1.5
  * Updated dxpy package to latest version due to SSH / SSL issues

* v1.1.4
  * Added a new method `bgzip_and_tabix` that automatically bgzips and tabix indexes a .tsv file and returns Path-objects for both
  * Refactored all Path.rename to Path.replace to avoid OS-specific issues
  * Made `get_chromosome()` log messages make slightly more sense
  * Added better comments for `genetics_loader()`
  * Fixed a bug in the plink sample counter to ensure proper reporting of number of individuals in an association run and simplified the code
  * Added an `import_lib.py` package to contain various import resources
    * This will continue to grow as methods are refactored from various packages into a single file of import resources
  * define_covariate_string() can now correct for array batch if requested

* v1.1.3
  * Bug fix for `download_dxfile_by_name`. Function was still returning a string.

* v1.1.2
  * `download_dxfile_by_name` now accepts a `Path` and returns a `Path` instead of a string.
  * Implemented `MRCLogger` for all methods in `association_resources.py`
    * Minor changes to run_cmd() to facilitate this change (which had already implemented MRCLogger)
  * MRCLogger should now function properly outside the DNANexus environment
  * Began adding better documentation and tests for individual methods in the `association_resources` module
  * `process_bgen_file` in `association_resources` has been simplified

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