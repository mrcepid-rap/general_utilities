import csv
import tarfile

import dxpy

from pathlib import Path
from typing import Union, Dict, Tuple, List

from general_utilities.association_resources import BGENInformation, download_dxfile_by_name, run_cmd
from job_management.command_executor import CommandExecutor, DockerMount


class DXPath:
    """A simple helper class the stores both remote DX and local file Path objects

    This is technically a wrapper around Path that stores an additional Pathlike describing the location of the
    file on the DNANexus platform. .local and .remote access the local and remote Paths, respectively.

    :param remote_path: Path on the DNANexus filesystem
    :param local_path: Path on the local AWS instance, if not provided, will be placed in the root directory with the
        name given by :func:`remote.name`

    :ivar remote: Path on the DNANexus filesystem
    :ivar local: Path on the local AWS instance
    """

    def __init__(self, remote_path: Union[str, Path], local_path: Union[str, Path] = None):

        self.remote = Path(remote_path)
        if local_path is None:
            self.local = Path(f'./{self.remote.name}')
        else:
            self.local = Path(local_path)


def build_default_command_executor(additional_mounts: List[DockerMount] = None) -> CommandExecutor:
    """Set up the 'CommandExecutor' class, which handles downloading a Docker image, building the appropriate
    file system mounts, and provides methods for running system calls.

    :param additional_mounts: Additional mount-points to add to this executor build
    :return: A CommandExecutor object
    """

    default_mounts = [DockerMount(Path('/home/dnanexus/'), Path('/test/'))]
    default_mounts.extend(additional_mounts)
    cmd_executor = CommandExecutor(docker_image='egardner413/mrcepid-burdentesting:latest',
                                   docker_mounts=default_mounts)

    return cmd_executor

def ingest_wes_bgen(bgen_index: dxpy.DXFile) -> Dict[str, BGENInformation]:
    """Download the entire filtered WES variant data set in bgen format

    The format of this file must be like the following (all spaces are tabs):

    chrom   vep_dxid   bgen_dxid    bgen_index_dxid   sample_dxid
    1    file-1234567890ABCDEFGH   file-0987654321ABCDEFGH   file-1234567890HGFEDCBA   file-0987654321HGFEDCBA

    :param bgen_index: A DNANexus file reference (file-12345) pointing to a TSV-format 'dictionary' of file paths
    :return: A Dict with keys of chromosomes and values of the BGENInformation typeddict
    """

    # Download the INDEX of bgen files:
    dxpy.download_dxfile(bgen_index.get_id(), "bgen_locs.tsv")
    # and load it into a dict:
    Path('filtered_bgen/').mkdir(exist_ok=True)  # For downloading later...

    with Path('bgen_locs.tsv').open('r') as bgen_index:
        bgen_index_csv = csv.DictReader(bgen_index, delimiter='\t')
        bgen_dict: Dict[str, BGENInformation] = dict()
        for line in bgen_index_csv:
            bgen_dict[line['chrom']] = {'index': dxpy.DXFile(line['bgen_index_dxid']),
                                        'sample': dxpy.DXFile(line['sample_dxid']),
                                        'bgen': dxpy.DXFile(line['bgen_dxid']),
                                        'vep': dxpy.DXFile(line['vep_dxid'])}

    return bgen_dict


def ingest_tarballs(association_tarballs: dxpy.DXFile,
                    named_tarball: dxpy.DXFile = None) -> Tuple[bool, bool, str, List[str]]:
    """Download the collapsed gene masks generated by mrcepid-collapsevariants

    This method will download either a single tarfile OR a list file of multiple tarfiles into this AWS instance. The
    files downloaded here MUST have been generated by the applet mrcepid-collapsevariants.

    The user can also provide a DXFile reference to a single tar file to check if a single, specific collapsed mask
    is present in the list file provided (association_tarballs) via the named_tarball parameter. If this parameter is
    provided, this method will provide a non-None string consisting of this files prefix to indicate that it has been
    found.

    :param association_tarballs: A single DXFile ID (file-12345...) or a list file containing multiple DXFile IDs
        pointing to tar.gz file(s) generated by mrcepid-collapsevariants.
    :param named_tarball: An optional DXFile reference pointing to a specific collapsed mask to check for in
        association_tarballs
    :return: A Tuple consisting of is_snp, is_gene, named_prefix, and a List of prefix(es) found in the provided file
    """
    is_snp_tar = False
    is_gene_tar = False
    named_prefix = None
    tarball_prefixes = []

    # First create a list of DNANexus fileIDs to process
    tar_files = []

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

    # And then process them in order
    for tar_file in tar_files:
        current_tar = download_dxfile_by_name(tar_file, print_status=False)
        if tarfile.is_tarfile(current_tar):
            tarball_prefix = current_tar.name.replace('.tar.gz', '')
            tarball_prefixes.append(tarball_prefix)
            tar = tarfile.open(current_tar, 'r:gz')
            tar.extractall()

            if Path(f'{tarball_prefix}.SNP.BOLT.bgen').exists():
                is_snp_tar = True
            elif Path(f'{tarball_prefix}.GENE.BOLT.bgen').exists():
                is_gene_tar = True

            # Check if the same as the interaction tarball:
            if named_tarball is not None:
                if named_tarball.describe()['name'] == current_tar.name:
                    named_prefix = named_tarball.describe()['name'].replace('.tar.gz', '')

        else:
            raise dxpy.AppError(f'Provided association tarball ({tar_file}) '
                                f'is not a tar.gz file')

    return is_snp_tar, is_gene_tar, named_prefix, tarball_prefixes


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
