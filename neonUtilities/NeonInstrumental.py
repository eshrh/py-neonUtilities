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

from . import neon
import re
import os
from os.path import join
import shutil
from itertools import chain

class NeonInstrumental(neon.Neon):
    def __init__(self, dpID=None, site=None, dates=None, avg = None, package="basic", token=None):
        #inherit functions from the parent Neon class from neon.py
        if type(avg)==int:
            avg = str(avg)

        neon.Neon.__init__(self, dpID, site, dates, avg, package, token)
        self.stackedFiles = {}
        if avg:
            self.isre = re.compile("[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.(.*)_"+avg+"min")
        else:
            self.isre = re.compile("[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.(.*)min")

    def download(self):
        """Class method to download zip files"""
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
            self.downloadFiles(idxurl,re=self.isre)
        print("Done downloading.")

    def stackByTable(self,root=None):
        if not root:
            self.root = join(os.getcwd(),self.rootname)
        else:
            self.root = join(os.getcwd(),root)
            self.folders = os.listdir(self.root)

        if len(self.folders)==0:
            print("No files stacked. Use download() or pass the folder path to stackByTable.")
            return

        self.stackedDir = join(os.getcwd(), root,"stackedFiles")

        if os.path.exists(self.stackedDir):
            shutil.rmtree(self.stackedDir)
        os.makedirs(self.stackedDir)

        self.folders = sorted(self.folders)
        self.files = []

        for i in self.folders:
            self.files.append([join(i,j) for j in os.listdir(join(root,i))])
        self.stack_site_date()

    def extractISname(self,s):
        s = os.path.basename(s)
        match = self.isre.search(s)
        if not match:
            return None
        matchstr = str(match.group(0))
        return matchstr[20:]

    def stack_site_date(self):
        """stacks site-date files. requires self.files to have been pregenerated.
        Outputs to self.stackedFiles.
        """
        # TODO site date is not always common between sites.
        flat = set(
            [self.extractISname(i) for i in list(chain.from_iterable(self.files))]
        )
        for name in flat:
            filename = join(self.stackedDir, name + "_stacked.csv")
            out = neon.CSVwriter(filename)
            for other in range(len(self.files)):
                for i in self.files[other]:
                    if name in i:
                        out.append(join(self.root, i))
                        break
            out.close()
            self.stackedFiles[name] = filename


