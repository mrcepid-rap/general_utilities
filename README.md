# General Utilities

## Changelog

* v1.5.0
  * Changes to support newer versions of plink2 that require sex to be in the sample file:
    * Added a new method `fix_plink_bgen_sample_sex` to `association_resources` that makes a plink2 compatible sample file
        * **NOTE**: This sample file codes all individuals as female! This is to ensure that a given .bgen can be processed by plink2. Please ensure that this does not create erroneous data when performing association tests!
    * Added a new method `sample_v2_to_v1` to `import_lib` for converting a v2 sample file to a v1 sample file
  * Small modifications to plot_lib to make plots clearer in some circumstances:
    * qq plots now should not label all markers for imputed data
    * Index variant text labels should now also be filtered by MAF

* v1.4.0
  * Changed expected python version from ~3.8 to ^3.8 in poetry
  * Added a fix for how indicies are found in the `find_index` method in `association_resources`
  * Removed the `run_cmd()` method from `association_resources` as it is now handled by the `CommandExecutor` class
  * Modified the `ingest_tarballs` method in `association_resources` to no longer accept a 'named tarball' to search for. This was done to remove complexity from the method.
  * Added new methods to `association_resources` :
    * `find_dxlink`: Searches for a dxlink in a project and returns the file. This is a simple wrapper around `dxpy.find_one_data_object`.
    * `check_gzipped`: Checks if a file is gzipped and returns an open file-handle using the appropriate method; `gzip.open()` for gzip, `Path.open()` for non-gzipped files.

* v1.3.0
  * Large number of changes to the SubjobUtility class:
    * Changed the parameter dereference_files to download_on_complete to better reflect what the parameter does
    * Unlinked log_update_time from how often the class checks for completed jobs to ensure that jobs aren't waiting for too long. Class now always checks every 60s for finished jobs.
    * Rebuilt how outputs are processed when jobs complete. All outputs are now dereferenced by dxpy.describe() so this does not have to be done by implementing classes. 
    * Fixed a bug where provided instance type wasn't actually used
    * Added doc strings for all methods / classes
    * Added / modified tests for new functionality
  * Added a 'delete_on_upload' parameter to the `generate_linked_dx_file` method in association_resources as an optional parameter. Default behaviour is as before, which is to delete the file on upload (default = True).
  * The 'build_transcripts_table' method now also creates the mean_chr_pos.tsv file for use during plotting of Manhattan plots.
  * Changed the 'get_sample_count' method to 'get_include_sample_ids'. This method now returns a list of sample IDs that are included in the analysis. To get the number of samples included, use the `len()` function on the returned list.
  * Added a 'dxfuse' package, which wraps the [dxfuse](https://github.com/dnanexus/dxfuse) command-line tool to mount a DXProject remote file system on an AWS node.
  * Removed the 'DXPath' class in import_utils. The class was causing confusion due to similarities with how DXLinks were used. All DXPath objects are now just dicts.
  * Updated the aws_credentials functionality of CommandExecutor to use a provided credentials file, rather than a DX resource provided to the applet .json.
  * Slightly changed how glm outputs are processed to use Path instead of os.
  * Added manhattan plotter methods to the plot_lib package

* v1.2.2
  * Changes to how genetic data is loaded due to format changes in the SAMPLE file
  * Several fixes to subjob utility
    * Added a `log_update_time` parameter to avoid printing too much information for longer-running jobs
    * Implemented MRCLogger functionality
    * Fixed apparent changes in how dxpy handles new subjobs as a call to the `new()` method of a DXJob object, rather than to the DXJob class
  * Fixed a bug in cluster_plotter where subsampling for R2 calculation purposes was not handled properly
  * Added a `project_id` parameter to download_dx_file_by_name that accepts a DNANexus project ID

* v1.2.1a
  * Emergency bug fix for the SAMPLES_Exclude file

* v1.2.1
  * Large number of general changes affecting all runassociationtesting modules. Please see the [RunAssociationTesting README](https://github.com/mrcepid-rap/mrcepid-runassociationtesting/blob/main/Readme.md) for more information
  * MRCLogger now saves a file containing all log messages printed to it
    * This DOES NOT save log messages printed by the underlying DNANexus logger
  * Large refactor of plot_lib
    * A 'Plotter' interface has been created to facilitate creating classes that make individual plot types

* v1.2.0
  * Updated docstrings for several methods
  * Implemented the CommandExecutor class
    * This class provides a standard method for importing Docker files and running commands either via Docker or through standard system calls
    * The original run_cmd() method still exists due to legacy code, but should be considered deprecated for future development, has been removed from all runassociationtesting modules, and is actively being removed from other applets.
    * This was done to:
      * Make it easier to run tests on local machines rather than via DNANexus
      * Standardize import of Docker images across modules / applets
      * Simplify calls to run_cmd()
  * Refactored bgen processing to import_lib

* v1.1.8
  * scipy is now hard-set to v1.10.1 due to issues with development version install and python < 3.9
  * run_cmd now returns the process exit code
  * Fixed a bug with MRCLogger printing multiple messages when not run on DNANexus
  * Refactored R packages for linear models as a sub-package of linear_model
  * Initial implementation of manhattan plot workflow

* v1.1.7
  * Null models frames now only include the phenotype for that model. Before frames just copied the larger pheno_covar.tsv
  * Removed overly verbose model printing

* v1.1.6
  * Added a skip_row parameter to bgzip_and_tabix for the -S parameter of tabix
  * Modernised process_model_output to return Paths and use bgzip_and_tabix()

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