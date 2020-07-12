# This file is part of py-neonUtilities.

# Foobar is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Foobar is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with py-neonUtilities.  If not, see <https://www.gnu.org/licenses/>.

from importlib import reload
import neon

reload(neon)

import re
import os
from os.path import join
import shutil
from itertools import chain
import zipfile
from pathlib import Path


class NeonInstrumental(neon.Neon):
    def __init__(
        self, dpID=None, site=None, dates=None, avg=None, package="basic", token=None
    ):
        # inherit functions from the parent Neon class from neon.py
        if type(avg) == int:
            avg = str(avg)

        neon.Neon.__init__(self, dpID, site, dates, avg, package, token)
        self.stackedFiles = {}
        if avg:
            self.isre = re.compile(
                "[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.(.*)_" + avg + "min"
            )
        else:
            self.isre = re.compile("[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.(.*)_(.*)")

    def download(self):
        """Class method to download zip files. Overridden."""
        self.rootname = self.data["dpID"]

        # create dataproduct directory if it does not exist
        # TODO allow for multiple dataproducts to be downloaded at once and stacked.
        if self.makeIfNotExists(self.rootname):
            print(f"[created root folder {self.rootname}]")

        # find all the idxurls of month-chunks
        self.idxurls = self.constructIdxUrls()
        print(f"{len(self.idxurls)} file(s) in total")
        self.zipfiles = []
        for n, idxurl in enumerate(self.idxurls):
            self.currentlyDl = n
            print(f"Downloading chunk {n+1}")
            if self.data["avg"]:
                self.downloadFiles(idxurl, re=self.isre)
            else:
                self.downloadZips(idxurl)
        print("Done downloading.")

    def unzipAll(self, root):
        folders = []
        self.zipfiles = sorted(self.zipfiles)
        # unzip all. sorted to make sure everything is in order.
        for fpath in self.zipfiles:
            with zipfile.ZipFile(fpath, "r") as f:
                Path(join(self.root, fpath[:-4])).mkdir(parents=True, exist_ok=True)
                f.extractall(join(self.root, fpath[:-4]))
                folders.append(join(self.root, fpath[:-4]))
        return folders

    def stackByTable(self, root=None, clean=True):
        if not root:
            self.root = join(os.getcwd(), self.rootname)
        else:
            self.root = join(os.getcwd(), root)
            self.folders = os.listdir(self.root)

        if len(self.folders) == 0 and len(self.zipfiles) == 0:
            print(
                "No files stacked. Use download() or pass the folder path to stackByTable."
            )
            return
        elif len(self.folders) == 0 and len(self.zipfiles) > 0:
            self.folders = self.unzipAll(self.root)

        self.stackedDir = join(os.getcwd(), self.root, "stackedFiles")
        self.resetDir(self.stackedDir)

        self.folders = sorted(self.folders)
        self.files = []

        for i in self.folders:
            self.files.append([join(i, j) for j in os.listdir(join(self.root, i))])

        flat = set(
            [self.extractISname(i) for i in list(chain.from_iterable(self.files))]
        )
        self.stack_site_date(self.files,flat)

        if clean:
            self.cleandir(self.root)

    def extractISname(self, s):
        s = os.path.basename(s)
        match = self.isre.search(s)
        if not match:
            return None
        matchstr = str(match.group(0))
        return matchstr.split(".")[-5]

def test():
    n = NeonInstrumental(
        dpID="DP1.00003.001", site="MOAB", dates=["2018-05", "2018-06"]
    )
    n.download()
    n.stackByTable()


test()
