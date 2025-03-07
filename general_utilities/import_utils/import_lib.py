import csv
import re
import shutil
import tarfile
from pathlib import Path
from typing import Dict, Tuple, List, TypedDict, Optional, Union

import dxpy

from general_utilities.association_resources import download_dxfile_by_name
from general_utilities.mrc_logger import MRCLogger

LOGGER = MRCLogger().get_logger()


class BGENInformation(TypedDict):
    """A TypedDict holding information about a chromosome's available genetic data

    :cvar bgen: A .bgen file containing genetic data.
    :cvar index: A .bgen.bgi index file for :cvar bgen:
    :cvar sample: A .sample file containing sample information for :cvar bgen:
    :cvar vep: The per-variant annotation for all or a filtered subset (typically on INFO / MAF) variants in :cvar bgen:
    :cvar vepidx: The index for the per-variant VEP annotation
    """
    bgen: dict
    index: dict
    sample: dict
    vep: Optional[dict]
    vepidx: Optional[dict]


def process_bgen_file(chrom_bgen_index: BGENInformation, chromosome: str) -> None:
    """Download and process a bgen file when requested

    This method is written as a helper to classes that need to access filtered and annotated WES variants. It will
    first download the files provided to `chrom_bgen_index`. It will then create a plink- / association
    software-compatible sample file. Finally, if requested, the method will filter to samples from the
    SAMPLES_Include.txt file generated after processing phenotypes / covariates.

    :param chrom_bgen_index: An object of :func:`BGENInformation` containing :func:`dxpy.DXFile` objects for the bgen
        for :param chromosome:
    :param chromosome: Which chromosome to limit analyses to
    :return: None
    """

    # First we have to download the actual data
    bgen_index = chrom_bgen_index['index']
    bgen_sample = chrom_bgen_index['sample']
    bgen = chrom_bgen_index['bgen']
    vep = chrom_bgen_index['vep']

    if isinstance(bgen, dxpy.bindings.dxfile.DXFile):

        dxpy.download_dxfile(bgen_index, f'filtered_bgen/{chromosome}.filtered.bgen.bgi')
        dxpy.download_dxfile(bgen_sample, f'filtered_bgen/{chromosome}.filtered.sample')
        dxpy.download_dxfile(bgen, f'filtered_bgen/{chromosome}.filtered.bgen')
        dxpy.download_dxfile(vep, f'filtered_bgen/{chromosome}.filtered.vep.tsv.gz')

        # Make a plink-compatible sample file (the one downloaded above is in bgen sample-v2 format)
        sample_v2_to_v1(Path(f'filtered_bgen/{chromosome}.filtered.sample'))

    else:
        shutil.copy(bgen_index, f'filtered_bgen/{chromosome}.filtered.bgen.bgi')
        shutil.copy(bgen_sample, f'filtered_bgen/{chromosome}.filtered.sample')
        shutil.copy(bgen, f'filtered_bgen/{chromosome}.filtered.bgen')
        shutil.copy(vep, f'filtered_bgen/{chromosome}.filtered.vep.tsv.gz')


def sample_v2_to_v1(bgen_v2: Path) -> Path:
    """Helper method to convert a bgenix / qctool v2 sample file to a plink-compatible v1 sample file

    This method converts the following sample file with three column header:

        ID missing sex
        0 0 D

    OR

        ID_1 ID_2 missing
        0 0 0

    To:

        ID_1 ID_2 missing sex
        0 0 0

    :param bgen_v2: A Path object pointing to a bgenix / qctool v2 sample file.
    :return: A Path object pointing to the newly created plink-compatible v1 sample file.
    """

    bgen_v1 = bgen_v2.with_suffix('.v1.sample')

    with bgen_v2.open('r') as samp_file, \
            bgen_v1.open('w') as fixed_samp:

        found_header_1 = False
        found_header_2 = False

        for line in samp_file:
            line = line.rstrip().split(" ")
            if line[0].startswith('ID'):
                found_header_1 = True
                fixed_samp.write('ID_1 ID_2 missing sex\n')
            elif line[0] == '0':
                found_header_2 = True
                fixed_samp.write('0 0 0 D\n')
            else:
                fixed_samp.write(f'{line[0]} {line[0]} 0 NA\n')

        if not found_header_1 or not found_header_2:
            raise dxpy.AppError(f'Provided bgen sample file ({bgen_v2}) is not in v2 format')

    return bgen_v1.replace(bgen_v2)


def ingest_wes_bgen(bgen_index: dxpy.DXFile) -> Dict[str, BGENInformation]:
    """Download the entire filtered WES variant data set in bgen format

    The format of this file must be like the following (all spaces are tabs):

    chrom   vep_dxid   bgen_dxid    bgen_index_dxid   sample_dxid
    1    file-1234567890ABCDEFGH   file-0987654321ABCDEFGH   file-1234567890HGFEDCBA   file-0987654321HGFEDCBA

    :param bgen_index: A DNANexus file reference (file-12345) pointing to a TSV-format 'dictionary' of file paths
    :return: A Dict with keys of chromosomes and values of the BGENInformation typeddict
    """

    # load filtered bgen info into a dict
    Path('filtered_bgen/').mkdir(exist_ok=True)  # For downloading later...

    # a workaround for non-DNA Nexus files
    # if the filenames of the bgens start with 'file-xxxxxx' it's a DNA Nexus file
    if isinstance(bgen_index.name, dxpy.bindings.dxfile.DXFile):

        # therefore run the DNA Nexus file parser
        dxpy.download_dxfile(bgen_index.get_id(), "bgen_locs.tsv")

        with Path('bgen_locs.tsv').open('r') as bgen_index:
            bgen_index_csv = csv.DictReader(bgen_index, delimiter='\t')
            bgen_dict: Dict[str, BGENInformation] = dict()
            for line in bgen_index_csv:
                bgen_info: BGENInformation = {'index': dxpy.dxlink(line['bgen_index_dxid']),
                                              'sample': dxpy.dxlink(line['sample_dxid']),
                                              'bgen': dxpy.dxlink(line['bgen_dxid']),
                                              'vep': dxpy.dxlink(line['vep_dxid']),
                                              'vepidx': dxpy.dxlink(line['vep_index_dxid'])}
                bgen_dict[line['chrom']] = bgen_info

    else:

        # otherwise, parse it locally
        with Path(bgen_index).open('r') as bgen_index:
            bgen_index_csv = csv.DictReader(bgen_index, delimiter='\t')
            bgen_dict: Dict[str, BGENInformation] = dict()
            for line in bgen_index_csv:
                bgen_info: BGENInformation = {'index': line['bgen_index_dxid'],
                                              'sample': line['sample_dxid'],
                                              'bgen': line['bgen_dxid'],
                                              'vep': line['vep_dxid'],
                                              'vepidx': line['vep_index_dxid']}
                bgen_dict[line['chrom']] = bgen_info

    return bgen_dict


def ingest_tarballs(association_tarballs: dxpy.DXFile) -> Tuple[bool, bool, List[str]]:
    """Download the collapsed gene masks generated by mrcepid-collapsevariants

    This method will download either a single tarfile OR a list file of multiple tarfiles into this AWS instance. The
    files downloaded here MUST have been generated by the applet mrcepid-collapsevariants.

    :param association_tarballs: A single DXFile ID (file-12345...) or a list file containing multiple DXFile IDs
        pointing to tar.gz file(s) generated by mrcepid-collapsevariants.
    :return: A Tuple consisting of is_snp, is_gene, named_prefix, and a List of prefix(es) found in the provided file
    """
    is_snp_tar = False
    is_gene_tar = False
    tarball_prefixes = []

    # First create a list of DNANexus fileIDs to process
    tar_files = []

    # check if we are working with a DNA Nexus file or not
    # if we are then process it like a DNA Nexus file
    if isinstance(input_filetype_parser(association_tarballs), dxpy.DXFile):
        dna_nexus_run = True

        # association_tarballs likely to be a single tarball:
        if '.tar.gz' in association_tarballs.describe()['name']:
            tar_files.append(association_tarballs.describe()['id'])

        # association_tarballs likely to be a list of tarballs:
        else:
            tarball_list = download_dxfile_by_name(association_tarballs, print_status=False)
            with tarball_list.open('r') as tarball_reader:
                for association_tarball in tarball_reader:
                    association_tarball = association_tarball.rstrip()
                    tar_files.append(association_tarball)

    # otherwise process it as a local file
    else:
        tar_files.append(association_tarballs)
        # and make a not that this is not a DNA Nexus run
        dna_nexus_run = False

    # And then process them in order
    for tar_file in tar_files:
        if dna_nexus_run:
            current_tar = download_dxfile_by_name(tar_file, print_status=False)
        else:
            current_tar = tar_file
        if tarfile.is_tarfile(current_tar):
            tarball_prefix = current_tar.name.replace('.tar.gz', '')
            tarball_prefixes.append(tarball_prefix)
            tar = tarfile.open(current_tar, 'r:gz')
            tar.extractall()

            # Construct regex dynamically from tarball_prefixes
            prefix_pattern = "|".join(
                re.escape(prefix) for prefix in tarball_prefixes)  # Escape in case of special characters
            pattern = rf'^(?:{prefix_pattern})\.([^.]+?)\.(?:BOLT|REGENIE|SAIGE|STAAR)'
            # Extract matches
            matches = set()
            for name in tar.getnames():
                match = re.search(pattern, name)
                if match:
                    extracted_value = match.group(1)  # Extract the second part after the prefix
                    if extracted_value not in {"SNP", "GENE"}:  # Remove SNP and GENE
                        matches.add(extracted_value)

            if Path(f'{tarball_prefix}.SNP.BOLT.bgen').exists():
                is_snp_tar = True
            elif Path(f'{tarball_prefix}.GENE.BOLT.bgen').exists():
                is_gene_tar = True

        else:
            raise dxpy.AppError(f'Provided association tarball ({tar_file}) '
                                f'is not a tar.gz file')

    return is_snp_tar, is_gene_tar, tarball_prefixes, matches


def process_regenie_step_one(regenie_run_location: dxpy.DXFile) -> bool:
    """A simple method to find, and if found, download a previously run REGENIE analysis.

    :param regenie_run_location: A possible DXFile reference to a previous regenie run
    :return: A boolean indicating whether step one regenie data was found
    """

    if regenie_run_location is None:
        return False
    else:
        tarball_path = download_dxfile_by_name(regenie_run_location)
        if tarfile.is_tarfile(tarball_path):
            tar = tarfile.open(tarball_path, 'r:gz')
            tar.extractall()
        return True


def input_filetype_parser(input_str: Optional[Union[str, Path, dxpy.DXFile]]) -> Optional[Union[dxpy.DXFile, Path]]:
    """
    Parses an input and returns the appropriate file type.

    - If input_str is None or "None", returns None.
    - If input_str is a dxpy.DXFile, returns it directly.
    - If input_str is a Path, processes it accordingly.
    - If input_str is a string matching a DNANexus file ID, returns a dxpy.DXFile.
    - Otherwise, treats input_str as an absolute file path:
         * First, attempts to locate the file on DNANexus.
         * If not found on DNANexus, checks if the file exists locally.
         * Returns a Path if it exists locally.

    :param input_str: The input representing a DNANexus file ID, a local file path, or already a dxpy.DXFile.
    :return: Either a dxpy.DXFile or a pathlib.Path.
    :raises FileNotFoundError: If the file is not found on DNANexus or locally.
    :raises ValueError: If a provided file path is not absolute.
    :raises TypeError: If a DNANexus file ID is invalid.
    """
    # Handle the case of a "None" input.
    if input_str is None or input_str == 'None':
        return None

    # If it's already a dxpy.DXFile or a Path, return it (or decide if you want to re-validate)
    if isinstance(input_str, dxpy.DXFile) or isinstance(input_str, Path):
        return input_str

    # Now, assume input_str is a string.
    # Case 1: DNANexus file ID.
    if re.fullmatch(r'file-\w{24}', input_str):
        try:
            dxfile = dxpy.DXFile(dxid=input_str)
            dxfile.describe()  # Validates that the file exists on DNANexus.
            return dxfile
        except (dxpy.exceptions.DXError, dxpy.exceptions.ResourceNotFound) as e:
            raise TypeError(f"Invalid DNANexus file ID: {input_str}") from e

    # Case 2: File path.
    file_handle = Path(input_str)
    if not file_handle.is_absolute():
        raise ValueError(f"Provided path '{input_str}' is not absolute. Please provide an absolute path.")

    try:
        # Attempt to locate the file on DNANexus.
        found_file = dxpy.find_one_data_object(
            classname='file',
            project=dxpy.PROJECT_CONTEXT_ID,
            name_mode='exact',
            name=file_handle.name,
            folder=str(file_handle.parent),
            zero_ok=False
        )
        return dxpy.DXFile(dxid=found_file['id'], project=found_file['project'])
    except dxpy.exceptions.DXSearchError:
        # If not found on DNANexus, check if the file exists locally.
        if file_handle.exists():
            logging.info(f"Local file '{input_str}' found.")
            return file_handle
        else:
            raise FileNotFoundError(
                f"File '{input_str}' not found on DNANexus or locally."
            )