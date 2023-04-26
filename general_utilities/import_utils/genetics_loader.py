import csv
from typing import Set, List

import dxpy

from pathlib import Path

from general_utilities.association_resources import run_cmd
from general_utilities.mrc_logger import MRCLogger


class GeneticsLoader:
    """Process genetic data from genotyping chips provided by UKBiobank

    :param bed_file: Plink .bed format file
    :param fam_file: Plink .fam format file
    :param bim_file: Plink .bim format file
    :param sample_file: A .bgen format sample file to synchronize samples on
    :param low_mac_list: An optional list of low minor allele count variants for exclusion when running BOLT
    """

    def __init__(self, bed_file: dxpy.DXFile, fam_file: dxpy.DXFile, bim_file: dxpy.DXFile, sample_files: List[Path],
                 low_mac_list: dxpy.DXFile = None):

        self._logger = MRCLogger(__name__).get_logger()

        self._bed_file = bed_file
        self._fam_file = fam_file
        self._bim_file = bim_file
        self._low_mac_list = low_mac_list

        self._ingest_genetic_data()
        if len(sample_files) > 0:  # Only do this is sample file(s) are provided to synchronise on
            self._logger.info('Multiple sample data types detected, synchronising sample lists...')
            self._union_sample = self._write_union_sample(sample_files)
            self._synchronise_genetic_data()
        self._generate_filtered_genetic_data()

    def _ingest_genetic_data(self) -> None:
        """Downloads provided genetic data in plink binary format to this instance.

        These files provided to this method should point to the processed genetic data curated by the mrcepid-makegrm
        applet of this workflow. An optional low_mac_list for BOLT can also be provided

        :return: None
        """

        # Now grab all genetic data that I have in the folder /project_resources/genetics/
        Path('genetics/').mkdir(exist_ok=True)  # This is for legacy reasons to make sure all tests work...
        dxpy.download_dxfile(self._bed_file.get_id(), 'genetics/UKBB_470K_Autosomes_QCd.bed')
        dxpy.download_dxfile(self._bim_file.get_id(), 'genetics/UKBB_470K_Autosomes_QCd.bim')
        dxpy.download_dxfile(self._fam_file.get_id(), 'genetics/UKBB_470K_Autosomes_QCd.fam')
        if self._low_mac_list is not None:
            dxpy.download_dxfile(self._low_mac_list.get_id(), 'genetics/UKBB_470K_Autosomes_QCd.low_MAC.snplist')
        self._logger.info('Genetic array data downloaded...')

    @staticmethod
    def _generate_sample_set(sample_path: Path) -> Set[str]:
        """Read a sample file and add all samples within it to a set()

        :param sample_path: Path to a sample file
        :return: A set containing string representations of all samples in the file passed to sample_path
        """

        sample_set = set()
        with sample_path.open('r') as sample_file:
            for sample in sample_file:
                sample = sample.rstrip().split()
                if sample[0] != "ID_1" and sample[0] != "0":
                    sample_set.add(sample[1])

        return sample_set

    def _write_union_sample(self, sample_paths: List[Path]) -> Path:
        """Merge some number of  sample files into a single intersected sample file

        This method uses the :func:`_generate_sample_set` to load 'N' sample files into independent sets,
        finds the intersection of these sets and then writes that intersection to a new sample file. This file is
        returned as a Path representation. Files processed by this method must be in standard plink / bgen format.
        The union sample file written by this method is NOT safe for any external tool(s) and is meant to just pass
        through the GeneticsLoader() class if required.

        :param sample_paths: a List of Paths to sample files
        :return: A Path representation of the union sample file
        """

        union_samples = set()
        for sample_path in sample_paths:
            if len(union_samples) > 0:
                union_samples = union_samples.intersection(self._generate_sample_set(sample_path))
            else:
                union_samples = self._generate_sample_set(sample_path)

        union_sample_path = Path('union.sample')
        with union_sample_path.open('w') as union_file:
            union_file.write('ID_1 ID_2\n')
            union_file.write('0 0\n')
            for sample in union_samples:
                union_file.write(f'{sample} {sample}\n')

        return union_sample_path

    def _synchronise_genetic_data(self) -> None:
        """Synchronise imputed and genetic data to ensure correct individuals are in each file

        Sometimes the imputed/dosage/WES data may have fewer individuals than the genetics bed files. This method
        rectifies that.

        Specifically, this methid synchronises the samples of three files:
        1. SAMPLES_Include.txt (Genetics individuals processed down to those with phenotype / covariate information)
        2. dosage/imputed samples
        3. phenotypes_covariates.formatted.txt – not necessary to ask for these samples at first as they are
              identical to SAMPLES_Include.txt

        We also generate a remove_SAMPLES.txt for BOLT to make it faster for processing this data when we are running
        association tests.

        :return: None
        """

        # 1. Read in imputed / dosage samples and get an intersection with SAMPLES_Include.txt
        include_path = Path('SAMPLES_Include.txt')
        with self._union_sample.open('r') as sample_file, \
                include_path.open('r') as include_file:

            testing_samples = set()
            for sample in sample_file:
                sample = sample.rstrip().split()
                if sample[0] != "ID_1" and sample[0] != "0":
                    testing_samples.add(sample[1])
            self._logger.info(f'{"Number of dosage / imputed samples":<40}: {len(testing_samples)}')

            # Genetic/covariate should be identical since we process them earlier, but just making sure here...
            genetic_samples = set()
            for sample in include_file:
                sample = sample.rstrip()
                genetic_samples.add(sample)

            self._logger.info(f'{"Number of .bed samples":<65}: {len(genetic_samples)}')
            valid_samples = testing_samples.intersection(genetic_samples)
            self._logger.info(f'{"Number of union samples":<65}: {len(valid_samples)}')

        # 2. Read in the processed phenotypes/covariates file and print out a new file based on the intersection with
        # 'valid_samples'.
        combo_path = Path('phenotypes_covariates.formatted.txt')
        new_include_path = Path('SAMPLES_Include.genetic_matched.txt')
        new_combo_path = Path('phenotypes_covariates.formatted_new.txt')

        with combo_path.open('r') as formatted_combo_file, \
                new_include_path.open('w') as new_include_file, \
                new_combo_path.open('w') as new_formatted_combo_file:

            # Here we just take this iteration opportunity to add in the 'array_batch' covariate for imputed data
            base_covar_reader = csv.DictReader(formatted_combo_file, delimiter=' ')
            indv_written = 0  # Just to count the number of samples we will analyse
            combo_writer = csv.DictWriter(new_formatted_combo_file,
                                          fieldnames=base_covar_reader.fieldnames,
                                          quoting=csv.QUOTE_NONE,
                                          delimiter=' ',
                                          lineterminator='\n')

            combo_writer.writeheader()
            for indv in base_covar_reader:
                if indv['FID'] in valid_samples:
                    combo_writer.writerow(indv)
                    new_include_file.write(f'{indv["FID"]}\n')
                    indv_written += 1

            self._logger.info(f'{"Number of INCLUDE samples":<65}: {indv_written}')

        new_combo_path.rename(combo_path)
        new_include_path.rename(include_path)

        # 3. Finally, we need to go back through the genetic data and write out samples that we need to exclude...
        # Read in valid samples from the imputed data, crosscheck the valid covariate samples, and write the
        # result to a new file:
        # I am unsure if this is necessary for the dosage format, but going to do it again to be sure...
        with self._union_sample.open('r') as sample_file, \
                Path(f'remove_SAMPLES.txt').open('w') as remove_file:

            num_exclude = 0
            for sample in sample_file:
                data = sample.rstrip().split()
                if data[1] not in valid_samples and data[0] != "ID_1" and data[0] != "0":
                    remove_file.write(f'{data[1]}\t{data[1]}\n')
                    num_exclude += 1

            self._logger.info(f'{"Number of REMOVE samples":<65}: {num_exclude}\n\n')

    def _generate_filtered_genetic_data(self):

        # Generate a plink file to use that only has included individuals:
        cmd = 'plink2 ' \
              '--bfile /test/genetics/UKBB_470K_Autosomes_QCd --make-bed --keep-fam /test/SAMPLES_Include.txt ' \
              '--out /test/genetics/UKBB_470K_Autosomes_QCd_WBA'
        run_cmd(cmd, is_docker=True, docker_image='egardner413/mrcepid-burdentesting')

        # I have to do this to recover the sample information from plink
        cmd = 'plink2 ' \
              '--bfile /test/genetics/UKBB_470K_Autosomes_QCd_WBA ' \
              '--validate'
        run_cmd(cmd, is_docker=True, docker_image='egardner413/mrcepid-burdentesting', stdout_file='plink_filtered.out')

        with Path('plink_filtered.out').open('r') as plink_out:
            for line in plink_out:
                self._logger.info(line)
                if 'loaded from' in line:
                    self._logger.info(f'{"Plink individuals written":{65}}: {line.rstrip(" loaded from")}')
