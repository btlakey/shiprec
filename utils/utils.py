from pathlib import Path
import os


def get_project_root(*args):
    """ Get (absolute) project root directory for saving to data/
    :param args: str, subdirectory names to check
    :return: str, absolute directory path
    """
    root = Path(__file__).parent.parent
    if args:
        checkdir = os.path.join(root, args[0])
    if os.path.isdir(checkdir):
        return checkdir
    else:
        print(f"{checkdir} does not exist, returning root")
        return root


def not_none(obj):
    """ Check whether an object is None """
    return obj is not None


def makedir_check(dirpath):
    """ Check if directory exists; if not, make
    :param dirpath: str, absolute directory path to check/make
    :return: None
    """
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

