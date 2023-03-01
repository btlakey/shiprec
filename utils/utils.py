from pathlib import Path
import os


def get_project_root(*args):
    root = Path(__file__).parent.parent
    if args:
        checkdir = os.path.join(root, args[0])
    if os.path.isdir(checkdir):
        return checkdir
    else:
        print(f"{checkdir} does not exist, returning root")
        return root
