import csv
import re
import shutil
import tarfile
from pathlib import Path
from typing import Dict, Tuple, List, TypedDict, Optional, Union

import dxpy
from general_utilities.association_resources import download_dxfile_by_name
from general_utilities.mrc_logger import MRCLogger
from general_utilities.import_utils.module_loader.insmedinput import InsmedInput

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

    InsmedInput(bgen_index, download_now=False, destination=f'filtered_bgen/{chromosome}.filtered.bgen.bgi')
    InsmedInput(bgen_sample, download_now=False, destination=f'filtered_bgen/{chromosome}.filtered.sample')
    InsmedInput(bgen, download_now=False, destination=f'filtered_bgen/{chromosome}.filtered.bgen')
    InsmedInput(vep, download_now=False, destination=f'filtered_bgen/{chromosome}.filtered.vep.tsv.gz')

    # Make a plink-compatible sample file (the one downloaded above is in bgen sample-v2 format)
    sample_v2_to_v1(Path(f'filtered_bgen/{chromosome}.filtered.sample'))


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


def ingest_wes_bgen(bgen_index: Union[dxpy.DXFile, dict]) -> Dict[str, BGENInformation]:
    """Download the entire filtered WES variant data set in bgen format

    The format of this file must be like the following (all spaces are tabs):

    chrom   vep_dxid   bgen_dxid    bgen_index_dxid   sample_dxid
    1    file-1234567890ABCDEFGH   file-0987654321ABCDEFGH   file-1234567890HGFEDCBA   file-0987654321HGFEDCBA

    :param bgen_index: A DNANexus file reference (file-12345) pointing to a TSV-format 'dictionary' of file paths
    :return: A Dict with keys of chromosomes and values of the BGENInformation typeddict
    """

    # load filtered bgen info into a dict
    Path('filtered_bgen/').mkdir(exist_ok=True)  # For downloading later...

    with Path(bgen_index).open('r') as bgen_index:
        bgen_index_csv = csv.DictReader(bgen_index, delimiter='\t')
        bgen_dict: Dict[str, BGENInformation] = dict()
        for line in bgen_index_csv:
            bgen_info: BGENInformation = {'index': InsmedInput(line['bgen_index_dxid'], download_now=False).file_handle,
                                          'sample': InsmedInput(line['sample_dxid'], download_now=False).file_handle,
                                          'bgen': InsmedInput(line['bgen_dxid'], download_now=False).file_handle,
                                          'vep': InsmedInput(line['vep_dxid'], download_now=False).file_handle,
                                          'vepidx': InsmedInput(line['vep_index_dxid'], download_now=False).file_handle}
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

    # association_tarballs likely to be a single tarball:
    if '.tar.gz' in str(association_tarballs):
        tar_files.append(association_tarballs)
    else:
        # association_tarballs likely to be a list of tarballs
        for file in association_tarballs:
            tarball = InsmedInput(file, download_now=False).file_handle
            tar_files.append(tarball)

    # Now process them in order
    for tar_file in tar_files:
        current_tar = InsmedInput(tar_file, download_now=True).file_handle
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


