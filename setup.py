from setuptools import setup

setup(name="data_processing",
      version="0.1.2",
      description="Data processing package for my code",
      author="jkurata",
      packages=["data_processing"],
      package_dir={"data_processing": "data_processing"},
      package_data={"data_processing": ["data/*.sam"]},
      include_package_data=True,
      install_requires=[
           "matplotlib", "matplotlib-venn", "pandas", "paramiko", "scipy"
      ],
      zip_safe=False)
