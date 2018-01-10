#!/usr/bin/env python

import tarfile
from glob import glob
import os

def load_messages(data_path, verbose=False):
    for tfile_name in sorted(glob(os.path.join(data_path,"*/*/*/*/*.tar.gz"))):
        if verbose:
            print "Opening tarfile:", tfile_name
        tfile = tarfile.open(tfile_name)
        for f_info in tfile.getmembers():
            if not f_info.isfile(): continue
            if not f_info.name.endswith(".msg"): continue

            if verbose:
                print "  Extracting message file:", f_info.name
            yield tfile.extractfile(f_info)

if __name__ == "__main__":
    data_path = os.environ.get('testdata.path', 'test-data')
    print "Looking in", data_path
    for msgfile in load_messages(data_path, True):
        print msgfile
        break
