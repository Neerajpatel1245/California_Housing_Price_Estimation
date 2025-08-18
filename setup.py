from setuptools import setup
from typing import List


# Declarinig variables for setup function
PROJECT_NAME = "House Predictor"
VERSION = "0.0.1"
AUTHOR = "Neeraj Patel"
EMAIL = "np899355@gmail.com"
DESCRIPTION = "This is machine learning project to predict the house price"
PACKAGES = ["housing"]  # list of folders where your project is stored or you want to make package
REQIUREMENT_FILE_NAME = "requirements.txt"


def get_requrements_list()->List[str]:
    """
    Description: This function is going to return the list of required libraries,
    mentioned in requirements.txt file
    """
    with open(REQIUREMENT_FILE_NAME) as requirement_file:
        return requirement_file.readlines()


setup(
    name=PROJECT_NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=EMAIL,
    description=DESCRIPTION,
    packages=PACKAGES,
    install_requires=get_requrements_list()
)
