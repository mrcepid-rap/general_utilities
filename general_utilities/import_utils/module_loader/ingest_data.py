import csv
import os
from abc import ABC
from pathlib import Path
from typing import Set, Tuple, List, Any, Dict

from general_utilities.import_utils.file_handlers.input_file_handler import InputFileHandler
from general_utilities.import_utils.module_loader.association_pack import AssociationPack, ProgramArgs
from general_utilities.job_management.command_executor import build_default_command_executor
from general_utilities.mrc_logger import MRCLogger


class IngestData(ABC):
    """Download and process required files and data to enable module functionality.

    This is an abstract class (ABC) so that different modules can access the methods within this class,
    but add additional imports as required in their own implementations. This interface does all of the processing of
    files required by all modules that use the associationtesting framework. This includes:

    1. The phenotype file
    2. The covariate file(s) – base and additional (if provided)
    3. Sample inclusion / exclusion lists
    4. Gene transcript dictionary

    :param parsed_options: A subclass of ProgramArgs containing options parsed by argparse
    """

    def __init__(self, parsed_options: ProgramArgs):

        # Initiate logger – This can also be used by a class which implements this Interface
        self._logger = MRCLogger(__name__).get_logger()

        self._parsed_options = parsed_options

        # Grab the Docker image, so we can run tools not on the DNANexus platform by default
        cmd_executor = build_default_command_executor()

        # Work our way through all the resources we need
        # Gene transcript dictionary
        transcript_index = self._ingest_transcript_index(parsed_options.transcript_index)

        # Phenotype files
        phenotypes = self._ingest_phenofile(parsed_options.phenofile, parsed_options.phenoname)
        pheno_names = list(phenotypes.keys())  # A list of pheno_names since we don't store phenotypes at runtime

        # Base and Additional covariates
        base_covariates_file, additional_covariates_file = self._ingest_covariates(parsed_options.base_covariates,
                                                                                   parsed_options.covarfile)

        # Sample inclusion / exclusion lists
        inclusion_filepath, exclusion_filepath = self._define_exclusion_lists(parsed_options.inclusion_list,
                                                                              parsed_options.exclusion_list)

        # Once all data is ingested, process the covariates/phenotypes into a single file of individuals that we want
        # to analyse
        genetics_samples = self._select_individuals(inclusion_filepath, exclusion_filepath, base_covariates_file)

        # Process additional covariates (check if requested in the function)
        found_categorical_covariates, found_quantitative_covariates, add_covars = \
            self._process_covariates(base_covariates_file,
                                     additional_covariates_file,
                                     parsed_options.categorical_covariates,
                                     parsed_options.quantitative_covariates,
                                     parsed_options.sex,
                                     parsed_options.ignore_base)

        # After all sample/phenotype/covariate processing, create the joint pheno/covariate file for testing
        final_covariates, inclusion_samples, exclusion_samples = self._create_covariate_file(
            genetics_samples=genetics_samples,
            phenotypes=phenotypes,
            pheno_names=pheno_names,
            covars=add_covars,
            covar_names=found_categorical_covariates + found_quantitative_covariates,
            sex=parsed_options.sex
        )

        # And build an object that will contain all the information we need to run some specified analysis
        self._association_pack = AssociationPack(is_binary=parsed_options.is_binary,
                                                 sex=parsed_options.sex,
                                                 threads=self._get_num_threads(),
                                                 pheno_names=pheno_names,
                                                 found_quantitative_covariates=found_quantitative_covariates,
                                                 found_categorical_covariates=found_categorical_covariates,
                                                 cmd_executor=cmd_executor,
                                                 final_covariates=final_covariates,
                                                 inclusion_samples=inclusion_samples,
                                                 exclusion_samples=exclusion_samples,
                                                 transcript_index=transcript_index
                                                 )

    def get_association_pack(self) -> AssociationPack:
        """Getter for self._association_pack

        :return: self._association_pack, which contains stored parameters necessary for running the current module
        """
        return self._association_pack

    def set_association_pack(self, objects: AssociationPack) -> None:
        """Setter for self._association_pack

        :param objects: An AssociationPack or subclass implementing AssociationPack
        :return: None
        """
        self._association_pack = objects

    def _get_num_threads(self) -> int:
        """Get number of cores available to the instance using :func:`os.cpu_count()`

        :return: The number of CPUs on this instance
        """

        threads = os.cpu_count()
        self._logger.info(f'{"Number of threads available":{65}}: {threads}')

        return threads

    @staticmethod
    def _ingest_transcript_index(transcript_index: InputFileHandler) -> Path:
        """Get transcripts for gene annotation

        The provided file with always be placed at `$HOME/transcripts.tsv.gz`.

        :param transcript_index: A transcript index in .tsv format. For more information on the structure of the file,
            see the README.
        :return: Path to the transcript index file
        """
        transcript_index = transcript_index.get_file_handle()
        return transcript_index

    def _ingest_phenofile(self, pheno_files: List[InputFileHandler], pheno_name: str) -> Dict[str, Dict[str, Any]]:
        """Download provided phenotype files and attempt to retrieve phenotypes from these file(s)

        There can be multiple phenotype files in some modes, so this method will iterate through the provided list
        and download all of them and store the respective file name in a List. This method with then process a (
        possible list of) phenotype file(s) (pheno_files) of the format:

                FID IID pheno1 pheno2 ... pheno3

        where a pheno_file can be either space or tab-delimited. This file **cannot** have blank phenotypes,
        but can have NA/NAN (in any case). This method will then parse a provided phenotype file and either:

        1. Extract all putative phenotypes (other than FID/IID) from pheno_file if pheno_name is NOT provided
        2. Extract a single provided pheno_name from a given pheno_file, else throw an error that the name wasn't found.

        Note that option (2) does not work when multiple pheno_files are provided and will raise an exception. Raw
        values from the provided pheno_file(s) are then extracted and placed into a dict() of phenotypes with a
        format of:

                {'pheno_name': {'1234567': 0.1,
                                '2345678': 0.3}}

        :param pheno_name: Phenotype name that MUST be in the header (csv.fieldnames) of the provided file.
        :param pheno_files: List of DXFile(s) pointing to provided pheno_files.
        :return: A dict with keys of pheno_name(s) found and values a dict with keys of IIDs and values of the
            given phenotype
        """

        # A dictionary of phenotypes for every individual in the phenofile
        phenotypes = {}

        for dx_pheno_file in pheno_files:

            pheno_file = dx_pheno_file.get_file_handle()

            total_missing_dict = {}
            total_samples = 0

            # Now process the downloaded pheno_file and extract phenotype names/raw phenotypes
            dialect = csv.Sniffer().sniff(pheno_file.open('r').readline(), delimiters=[' ', '\t'])
            with pheno_file.open('r') as pheno_reader:
                pheno_csv = csv.DictReader(pheno_reader, delimiter=dialect.delimiter)
                field_names = pheno_csv.fieldnames

                # Check to make sure we have a proper identifier
                if "FID" not in field_names or "IID" not in field_names:
                    raise ValueError("Pheno file does not contain FID/IID fields!")

                # And ingest individual phenofields...
                curr_pheno_names = []

                # If phenoname not provided, then ingest all possible phenotypes
                if pheno_name is None:
                    for field in field_names:
                        if field != "FID" and field != "IID":
                            curr_pheno_names.append(field)
                            total_missing_dict[field] = 0
                # Else just try and munge the one phenoname from the file
                else:
                    if pheno_name in field_names:
                        curr_pheno_names.append(pheno_name)
                        total_missing_dict[pheno_name] = 0
                    else:
                        raise ValueError("phenoname was not found in the provided phenofile!")

                # And then iterate through every sample in the pheno_file and add the information to our phenotypes
                # dictionary
                for indv in pheno_csv:
                    total_samples += 1
                    for pheno in curr_pheno_names:
                        if pheno not in phenotypes:
                            phenotypes[pheno] = {}

                        # Will spit out an error if a given sample is not formatted properly
                        if indv[pheno] is None:
                            raise ValueError("Phenotype file has blank lines!")

                        # Exclude individuals that have missing data (NA/NAN) from the dictionary (these will be
                        # properly added to the final covariate file as Null values later in the data ingestion
                        # process).
                        elif indv[pheno].lower() != "na" and indv[pheno].lower() != "nan" and indv[pheno].lower() != "":
                            phenotypes[pheno][indv['FID']] = indv[pheno]
                        else:
                            total_missing_dict[pheno] += 1

            for pheno in total_missing_dict:
                print_string = f'Total samples in file {pheno_file.name}'
                self._logger.info(f'{print_string:{65}}: {total_samples}')
                print_string = f'Phenotype "{pheno}" samples missing'
                prop_missing = (total_missing_dict[pheno] / total_samples) * 100
                self._logger.info(f'{" ":^{5}}{print_string:{60}}: {total_missing_dict[pheno]} ({prop_missing:0.2f}%)')

        return phenotypes

    @staticmethod
    def _ingest_covariates(base_covariates: InputFileHandler, covarfile: InputFileHandler) -> Tuple[Path, Path]:
        """Download covariate data

        Base covariates will always be placed at `$HOME/base_covariates.covariates`. Additional covariates (if provided)
        will always be placed at `$HOME/additional_covariates.covariates`.

        :param base_covariates: A DXFile pointing to the base_covariates file on the RAP
        :param covarfile: A DXFile (possibly None if not provided) pointing to the base_covariates file on the RAP
        :return: A boolean that is true if additional covariates beyond the base covariates were provided
        """

        # download the base covariates file
        base_covariates = base_covariates.get_file_handle()

        # Check if additional covariates were provided:
        if covarfile is not None:
            additional_covariates = covarfile.get_file_handle()
        else:
            # If no additional covariates were provided, set it to None
            additional_covariates = None

        return base_covariates, additional_covariates

    @staticmethod
    def _define_exclusion_lists(inclusion_list: InputFileHandler, exclusion_list: InputFileHandler) -> Tuple[Path, Path]:
        """Get inclusion/exclusion sample lists

        If provided, inclusion and exclusion lists will be downloaded to `$HOME/INCLUSION.lst` and
        `$HOME/EXCLUSION.list`, respectively.

        :param inclusion_list: A DXFile (possibly None if not provided) pointing to a sample inclusion list file on
            the RAP
        :param exclusion_list: A DXFile (possibly None if not provided) pointing to a sample inclusion list file on
            the RAP
        :return: A Tuple with two booleans, describing if an inclusion or exclusion list were found, respectively
        """

        if inclusion_list is not None:
            # get the filepath for the inclusion list
            inclusion_list = inclusion_list.get_file_handle()
        else:
            # if no inclusion list was provided, set it to None
            inclusion_list = None
        if exclusion_list is not None:
            # get the filepath for the exclusion list
            exclusion_list = exclusion_list.get_file_handle()
        else:
            # if no exclusion list was provided, set it to None
            exclusion_list = None

        return inclusion_list, exclusion_list

    def _select_individuals(self, inclusion_filepath: Path, exclusion_filepath: Path, base_covariates: Path) -> Set[str]:
        """Define individuals based on exclusion/inclusion lists.

        Three steps to this:

        1. Read inclusion samples
        2. Read exclusion samples
        3. Read samples with WES & Genetic data (by default encoded as the 'genetics_qc_pass' in the base_covariates file)
        4. Mash these three sources together to get a `set` of individuals we will include in downstream analysis

        The logic to step (4) is:

        * If inclusion + exclusion: Any individual to include in our study MUST be in the inclusion list and NOT in the exclusion list if both are provided.
        * If inclusion: a sample MUST be in this file to be included.
        * If exclusion: a sample CANNOT be in the file and be included.
        * If neither: All samples in base_covariates with genetics_qc_pass = PASS are included

        :param inclusion_filepath: Filepath to the inclusion list, if provided. If not provided, this is set to None.
        :param exclusion_filepath: Filepath to the exclusion list, if provided. If not provided, this is set to None.
        :param base_covariates: Filepath to the base_covariates file.
        :return: A set() containing eids of the samples for all downstream analysis
        """

        # 1. Get a list of individuals that we ARE going to use
        include_samples = set()
        if inclusion_filepath is not None:
            with open(inclusion_filepath, 'r') as inclusion_file:
                for indv in inclusion_file:
                    indv = indv.rstrip()
                    include_samples.add(indv)

        # 2. Get a list of individuals that we ARE NOT going to use
        exclude_samples = set()
        if exclusion_filepath is not None:
            with open(exclusion_filepath, 'r') as exclude_file:
                for indv in exclude_file:
                    indv = indv.rstrip()
                    exclude_samples.add(indv)

        # 3. Get individuals that are POSSIBLE to include (they actually have WES AND pass genetic data filtering) and
        # only keep 'include' samples.
        # Remember! the genetic data has already been filtered to individuals with WES data.

        # 4. Generate A set of all possible samples to include in this analysis
        genetics_samples = set()
        total_base_samples = 0

        with Path(base_covariates).open('r') as base_covariates_file:
            base_covar_reader = csv.DictReader(base_covariates_file, delimiter="\t")
            for indv in base_covar_reader:
                total_base_samples += 1
                eid = indv['FID']
                genetics_status = int(indv['genetics_qc_pass'])

                # extract the variable indicating whether an individual has passed genetic data QC:
                if genetics_status == 1:
                    if inclusion_filepath is None and exclusion_filepath is None:
                        genetics_samples.add(eid)
                    elif inclusion_filepath is None and exclusion_filepath is not None:
                        if eid not in exclude_samples:
                            genetics_samples.add(eid)
                    elif inclusion_filepath is not None and exclusion_filepath is None:
                        if eid in include_samples:
                            genetics_samples.add(eid)
                    else:
                        if eid in include_samples and eid not in exclude_samples:
                            genetics_samples.add(eid)
            base_covariates_file.close()

        self._logger.info(f'{"Total samples in base_covariates file":{65}}: {total_base_samples}')
        self._logger.info(f'{"Total samples after inclusion/exclusion lists applied":{65}}: {len(genetics_samples)}')
        return genetics_samples

    def _process_covariates(self,
                            base_covariates_path: Path,
                            additional_covariates_path: Path,
                            categorical_covariates: List[str],
                            quantitative_covariates: List[str],
                            sex: int,
                            ignore_base_covariates: bool
                            ) -> Tuple[List[str], List[str], Dict[str, Dict[str, Any]]]:
        """Identify additional covariates beyond base covariates and ensure all asked for additional covariate(s) are
        in the provided covariate file.

        Process a covariate file of the form:

                FID IID catCovar1 catCovar2 quantCovar3

        where the provided file can be either space or tab-delimited. This method is relatively, simple, and will
        iterate through the provided covariate file, ensuring that covariates that are asked for are in the provided
        file. This method will also report statistics on total / proportion of individuals missing each covariate.

        :param base_covariates_path: Path to the base covariate file.
        :param additional_covariates_path: Path to the additional covariate file, if provided.
        :param categorical_covariates: List of names of additional categorical covariates
        :param quantitative_covariates: List of names of additional quantitative covariates
        :param ignore_base_covariates: Boolean indicating whether to ignore base covariates in the analysis
        :return: A Tuple containing two sets (of categorical and quantitative covariate names, respectively) and a Dict
            with keys of sample IDs and values a Dict with keys of covariate names and values of covariate values.
        """

        self._logger.info('Processing additional covariates...')

        # Build a dict of all covariate information
        covars = {}
        # Keep track of sample totals:
        total_missing_dict = {}

        # Create sets of all covariates so we can use set logic rather than list logic on members
        base_categorical_covariates = {'batch', 'array_batch'}
        base_quantitative_covariates = set([f'PC{PC}' for PC in range(1, 11)] + \
                                           ['age', 'age_squared', 'sex'])

        additional_categorical_covariates = set(categorical_covariates)
        if 'sex' in additional_categorical_covariates:
            raise ValueError('If adding "sex", please add to --quantitative_covariates, not --categorical_covariates!')

        additional_quantitative_covariates = set(quantitative_covariates)

        # Check if we are ignoring base covariates, but also make sure we search if users are adding them back
        # to the model
        if ignore_base_covariates:
            self._logger.warning('Flag --ignore_base used. No base covariates included (unless specifically requested '
                                 'using --quantitative_covariates or --categorical_covariates)...')
            base_categorical_covariates = additional_categorical_covariates.intersection(base_categorical_covariates)
            base_quantitative_covariates = additional_quantitative_covariates.intersection(base_quantitative_covariates)

            # if sex is != 2, sex always has to be in covariates surveyed, but gets removed from the covariate string
            if sex != 2:
                base_quantitative_covariates.add('sex')

        # Probably unnecessary to do this, but make sure that we don't double count covariates
        additional_categorical_covariates = additional_categorical_covariates.difference(base_categorical_covariates)
        additional_quantitative_covariates = additional_quantitative_covariates.difference(base_quantitative_covariates)

        # Check delimiter of the csv file(s) algorithmically:
        base_dialect = csv.Sniffer().sniff(base_covariates_path.open('r').readline(), delimiters=[' ', '\t'])
        with base_covariates_path.open('r') as base_covariates_file:

            base_covar_csv = csv.DictReader(base_covariates_file, delimiter=base_dialect.delimiter)
            if 'FID' not in base_covar_csv.fieldnames or 'IID' not in base_covar_csv.fieldnames:
                raise ValueError('FID & IID column not found in provided base covariates file!')

            # A note for future developers: We collect _all_ samples here to ensure that lists are concurrent
            # across various sample sources & include/exclude lists.
            for sample in base_covar_csv:

                if 'sex' not in base_quantitative_covariates and sex != 2:
                    raise ValueError('--sex set to 0 / 1, but not included in covariates. Did you use --ignore_base and not '
                                     'include in --quantitative_covariates?')

                sample_dict = {}

                for covar_name in base_categorical_covariates.union(base_quantitative_covariates):
                    if covar_name == 'age_squared':  # Need a special carve out for age squared since it is not pre-computed
                        if 'age' not in sample:
                            raise ValueError('Requested base covariate "age_squared" but "age" not found in provided base covariate file!')
                        elif sample['age'] is None:
                            total_missing_dict[covar_name] += 1
                        elif sample['age'].lower() in ['na', 'nan', '']:
                            total_missing_dict[covar_name] += 1
                        else:
                            sample_dict['age_squared'] = int(sample['age']) ** 2
                    elif covar_name not in sample:
                        raise ValueError(f'Requested base covariate "{covar_name}" not found in provided base covariate file!')
                    elif sample[covar_name] is None:
                        total_missing_dict[covar_name] += 1
                    elif sample[covar_name].lower() in ['na', 'nan', '']:
                        total_missing_dict[covar_name] += 1
                    else:
                        if covar_name not in total_missing_dict:
                            total_missing_dict[covar_name] = 0
                        sample_dict[covar_name] = sample[covar_name]

                covars[sample['IID']] = sample_dict

        if additional_covariates_path:

            additional_dialect = csv.Sniffer().sniff(additional_covariates_path.open('r').readline(), delimiters=[' ', '\t'])
            with additional_covariates_path.open('r') as additional_covariates_file:
                additional_covar_csv = csv.DictReader(additional_covariates_file, delimiter=additional_dialect.delimiter)
                if 'FID' not in additional_covar_csv.fieldnames or 'IID' not in additional_covar_csv.fieldnames:
                    raise ValueError('FID & IID column not found in provided additional covariates file!')

                for sample in additional_covar_csv:

                    if sample['IID'] in covars:
                        sample_dict = covars[sample['IID']]
                    else:
                        sample_dict = {}

                    for covar_name in additional_categorical_covariates.union(additional_quantitative_covariates):
                        if covar_name not in sample:
                            raise ValueError(f'Requested additional covariate "{covar_name}" not found in provided additional covariate file!')
                        elif sample[covar_name] is None:
                            total_missing_dict[covar_name] += 1
                        elif sample[covar_name].lower() in ['na', 'nan', '']:
                            total_missing_dict[covar_name] += 1
                        else:
                            if covar_name not in total_missing_dict:
                                total_missing_dict[covar_name] = 0
                            sample_dict[covar_name] = sample[covar_name]

                    covars[sample['IID']] = sample_dict

        # Report total number of samples with missing covariates
        total_samples = len(covars)
        for covar_name in total_missing_dict:
            print_string = f'Covariate "{covar_name}" samples missing'
            prop_missing = (total_missing_dict[covar_name] / total_samples) * 100
            self._logger.info(f'{print_string:{65}}: {total_missing_dict[covar_name]} ({prop_missing:0.2f}%)')

        found_categorical_covariates = base_categorical_covariates.union(additional_categorical_covariates)
        found_quantitative_covariates = base_quantitative_covariates.union(additional_quantitative_covariates)

        # Confusing one, but if user has requested a single sex (e.g., female only analysis) we don't want that
        # written to our list of covariates, but it is required to be part of the per-sample information used for
        # filtering.
        if sex != 2:
            if 'sex' in found_quantitative_covariates:
                found_quantitative_covariates.remove('sex')

        self._logger.info(f'{"Default covariates included in model":{65}}:')
        if len(found_quantitative_covariates) + len(found_categorical_covariates) > 0:
            self._logger.info(f'{"Number of individuals with non-null covariates":{65}}: {len(covars)}')
            self._logger.info(f'{"Additional covariates included in model":{65}}:')
            self._logger.info(
                f'{" ":^{5}}{"Quantitative":{60}}: {", ".join(found_quantitative_covariates) if len(found_quantitative_covariates) > 0 else "None"}')
            self._logger.info(
                f'{" ":^{5}}{"Categorical":{60}}: {", ".join(found_categorical_covariates) if len(found_categorical_covariates) > 0 else "None"}')
            self._logger.info(
                f'Note that array_batch will only be used for array-based, and not sequencing-based analyses.')
        else:
            self._logger.info(f'No additional covariates provided/found...')

        return list(found_categorical_covariates), list(found_quantitative_covariates), covars

    def _create_covariate_file(self, genetics_samples: Set[str], phenotypes: Dict[str, Dict[str, Any]],
                               pheno_names: List[str], covars: Dict[str, Dict[str, Any]], covar_names: List[str],
                               sex: int) -> Tuple[Path, Path, Path]:

        """Print final covariate + phenotype file while implementing sample inclusion/exclusion

        While long, this method is relatively simple. Simple checking that all covariates and phenotypes for a given
        individual are present, and then writing that individual to the final covariates file. If multiple phenotypes
        are provided, individuals with missing phenotypes are coded as NA. Otherwise, individuals are excluded
        from the final file. For covariates, individuals must have ALL asked for and base covariates to to be written
        to the final pheno + covariate file (always located at `phenotypes_covariates.formatted.txt`).

        :param genetics_samples: List of valid sample IDs as determined by the combination of inclusion / exclusion
            lists and base covariate file(s)
        :param phenotypes: Dictionary of phenotypes with per-individual values
        :param pheno_names: List of pheno_names in phenotypes
        :param covars: Dictionary of additional covariates with per-individual values
        :param sex: Sex to restrict analysis to (either 0 [female], 1 [male], or 2 [both])
        :return: None
        """

        # Set the output files
        final_covariates_file = Path('phenotypes_covariates.formatted.txt')
        inclusion_samples = Path('SAMPLES_Include.txt')
        exclusion_samples = Path('SAMPLES_Exclude.txt')

        # Print some statistics about what we found in previous ingestion classes:
        self._logger.info(f'{"Phenotype(s)":{65}}: {", ".join(pheno_names)}')

        with final_covariates_file.open('w', newline='\n') as final_covariates_writer, \
                inclusion_samples.open('w') as include_samples, \
                exclusion_samples.open('w') as remove_samples:

            # Build fieldnames for final_covariates_file
            write_fields = ['FID', 'IID'] + pheno_names + covar_names

            # Open the final CSV writer
            combo_writer = csv.DictWriter(final_covariates_writer,
                                          fieldnames=write_fields,
                                          quoting=csv.QUOTE_NONE,
                                          delimiter=' ',
                                          extrasaction='ignore',
                                          lineterminator='\n')
            combo_writer.writeheader()

            num_all_samples = 0
            indv_written = 0  # Count the number of samples we WILL analyse
            indv_exclude = 0  # Count the number of samples we WONT analyse
            for sample_id in covars:

                # If genetic sex is not NA and the individual is in the genetics sample list, then we can write them
                if sample_id in genetics_samples:
                    # write a correctly formatted covariate file
                    indv_writer = covars[sample_id]
                    indv_writer.update({'FID': sample_id, 'IID': sample_id})
                    num_all_samples += 1

                    # Check if we found additional covariates and make sure this sample has no null values
                    if len(set(covar_names).intersection(set(indv_writer.keys()))) == len(covar_names):
                        found_covars = True
                    else:
                        found_covars = False

                    # Now check if phenos are full
                    found_phenos = True
                    for pheno_name in pheno_names:
                        if sample_id in phenotypes[pheno_name]:
                            indv_writer[pheno_name] = phenotypes[pheno_name][sample_id]
                        else:
                            found_phenos = False

                    # exclude based on sex-specific analysis if required:
                    # We use different logic depending on whether we have multiple phenotypes or not
                    if found_covars and ((len(pheno_names) == 1 and found_phenos) or len(pheno_names) > 1):
                        if sex == 2:
                            indv_written += 1
                            combo_writer.writerow(indv_writer)
                            include_samples.write(f'{sample_id} {sample_id}\n')
                        elif sex == int(indv_writer['sex']):
                            indv_written += 1
                            combo_writer.writerow(indv_writer)
                            include_samples.write(f'{sample_id} {sample_id}\n')
                        else:
                            remove_samples.write(f'{sample_id} {sample_id}\n')
                            indv_exclude += 1
                    else:
                        remove_samples.write(f'{sample_id} {sample_id}\n')
                        indv_exclude += 1
                else:
                    remove_samples.write(f'{sample_id} {sample_id}\n')
                    indv_exclude += 1

        # Print to ensure that total number of individuals is consistent between genetic and covariate/phenotype data
        self._logger.info(f'{"Samples with covariates after include/exclude lists applied":{65}}: {num_all_samples}')
        self._logger.info(f'{"Number of individuals WRITTEN to covariate/pheno file":{65}}: {indv_written}')
        self._logger.info(f'{"Number of individuals EXCLUDED from covariate/pheno file":{65}}: {indv_exclude}')

        return final_covariates_file, inclusion_samples, exclusion_samples
