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
    version='1.1.5',
    packages=find_packages(),
    url='https://github.com/mrcepid-rap/general_utilities',
    license='MIT',
    author='Eugene Gardner',
    author_email='eugene.gardner@mrcepid.cam.ac.uk',
    description='',
    install_requires=load_requirements(Path("requirements.txt")),
    include_package_data=True,
    package_data={'': ['R_resources/*.R']}
)
