from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

from general_utilities.import_utils.file_handlers.input_file_handler import InputFileHandler
from general_utilities.job_management.command_executor import CommandExecutor


@dataclass
class ProgramArgs(ABC):
    """A @dataclass that stores information on arguments that are passed to optparse

    This class is for ease of programming and has no actual functionality for any of the processing occurring in this
    workflow. It allows for coders to know possible input parameters and to get proper typehints when adding new
    functionality. For more information see either _load_module_options() in ModuleLoader (module_loader.py) or the
    implemented version of this method in individual modules subclasses that implement ModuleLoader.
    """

    phenofile: List[InputFileHandler]
    phenoname: str
    covarfile: InputFileHandler
    categorical_covariates: List[str]
    quantitative_covariates: List[str]
    is_binary: bool
    sex: int
    exclusion_list: InputFileHandler
    inclusion_list: InputFileHandler
    transcript_index: InputFileHandler
    base_covariates: InputFileHandler
    ignore_base: bool

    def __post_init__(self):
        """@dataclass automatically calls this method after calling its own __init__()"""
        self._check_opts()

    @abstractmethod
    def _check_opts(self):
        """An abstract method that allows for additional processing of options for each module"""
        pass


class AssociationPack(ABC):
    """An interface that stores information necessary for genetic association.

    All modules that can be run through RunAssociationTesting MUST implement this interface to store information
    # required for genetic association. The parameters stored in this interface are those required for all such modules
    (i.e., these parameters are required no matter what analysis is being performed) and are processed and implemented
    in the Additional subclasses can implement this interface to add additional parameters.

    :param is_binary: Is the phenotype binary?
    :param sex: Sex to perform sex-stratified analysis for. Possible values are 0 = Female, 1 = Male, 2 = Both.
    :param threads: Number of threads available to the current machine running this job.
    :param pheno_names: str names of phenotypes requested to be run for this analysis.
    :param ignore_base_covariates: Should base covariates (e.g., Sex, age) be ignored when running models?
    :param found_quantitative_covariates: str names of any found quantitative covariates.
    :param found_categorical_covariates: str names of any found categorical covariates.
    :param cmd_executor: Class to run system calls via the shell or Docker.
    """

    def __init__(self, is_binary: bool, sex: int, threads: int, pheno_names: List[str], ignore_base_covariates: bool,
                 found_quantitative_covariates: List[str], found_categorical_covariates: List[str],
                 cmd_executor: CommandExecutor, low_MAC_list: InputFileHandler, sparse_grm: InputFileHandler,
                 sparse_grm_sample: InputFileHandler):
        self.is_binary = is_binary
        self.sex = sex
        self.threads = threads
        self.pheno_names = pheno_names
        self.ignore_base_covariates = ignore_base_covariates
        self.found_quantitative_covariates = found_quantitative_covariates
        self.found_categorical_covariates = found_categorical_covariates
        self.cmd_executor = cmd_executor
        self.low_MAC_list = low_MAC_list
        self.sparse_grm = sparse_grm
        self.sparse_grm_sample = sparse_grm_sample
