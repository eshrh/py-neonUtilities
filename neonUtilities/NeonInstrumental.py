# This file is part of py-neonUtilities.

# py-neonUtilities is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# py-neonUtilities is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with py-neonUtilities.  If not, see <https://www.gnu.org/licenses/>.

from . import neon
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

        neon.Neon.__init__(self, dpID, site, dates, avg, package, token)
        self.stackedFiles = {}

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

    def stackByTable(self, root=None, clean=True, bySite=False):

        if root is None:
            self.root = join(os.getcwd(), self.rootname)
        else:
            self.folders = []
            self.root = join(os.getcwd(), root)
            folderfiles = os.listdir(self.root)
            for i in folderfiles:
                if os.path.isdir(i) and "stackedFiles" not in i:
                    self.folders.append(i)
                elif self.zipre.search(i):
                    self.zipfiles.append(join(root,i))

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

        if bySite:
            for site in self.data["site"]:
                self.stack_site_date(self.files, flat, site=site)
        else:
            self.stack_site_date(self.files, flat)

        if clean:
            self.cleandir(self.root)

    def extractISname(self, s):
        s = os.path.basename(s)
        match = self.isre.search(s)
        if not match:
            return None
        matchstr = str(match.group(0))
        if self.data["avg"] is None:
            return matchstr.split(".")[-5]
        else:
            return matchstr.split(".")[-1]
