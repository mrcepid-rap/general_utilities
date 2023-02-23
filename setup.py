from setuptools import setup, find_packages
from pathlib import Path


def load_requirements(fname: Path):
    reqs = []
    with fname.open('r') as reqs_file:
        for line in reqs_file:
            reqs.append(line.rstrip())
    return reqs


setup(
    name='general_utilities',
    version='',
    packages=find_packages(),
    url='',
    license='',
    author='ejgardner',
    author_email='',
    description='',
    install_requires=load_requirements(Path("requirements.txt")),
    include_package_data=True,
    package_data={'general_utilities.R_resources': ['R_resources/*.R']}
)
