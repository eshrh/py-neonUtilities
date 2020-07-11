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

import glob
from importlib import reload
import neon

reload(neon)

import os
from os.path import join
from pathlib import Path
import shutil
import zipfile
import re
from itertools import chain


class NeonObservational(neon.Neon):
    def __init__(self, dpID=None, site=None, dates=None, package="basic", token=None):
        # inherit functions from the parent Neon class from neon.py
        neon.Neon.__init__(self, dpID, site, dates, package, token)
        self.stackedFiles = {}

    def stackByTable(self, root=None, clean=True):
        """
        Method to stack zip files by table

        stackByTable can be used as a class method after downloading files, or you can provide a single argument root
        which is the path to the directory containing zip files. If used as a class method,
        it will create a directory named the dpID of your file. After stacking, the csv files containing
        data will be placed in <dpID>/stackedFiles. The csv name will follow the pattern DESC_stacked.csv.
        You can pass clean=False to not delete expanded files after stacking.
        """

        if not root:
            root = join(os.getcwd(), self.rootname)
        else:
            root = join(os.getcwd(), root)
            self.zipfiles = glob.glob(join(root, "*.zip"))

        if len(self.zipfiles) == 0:
            print(
                "No download files found. Pass the download directory to this function or use the download() method."
            )
            return

        # set the root to the default if the user gave nothing.
        self.root = (
            join(os.getcwd(), self.rootname) if not root else join(os.getcwd(), root)
        )

        # defaults to the equivalent in the R package for compatibility.
        self.stackedDir = join(os.getcwd(), root, "stackedFiles")

        # reset stackedDir.
        if os.path.exists(self.stackedDir):
            shutil.rmtree(self.stackedDir)
        os.makedirs(self.stackedDir)

        files = []
        self.zipfiles = sorted(self.zipfiles)
        # unzip all. sorted to make sure everything is in order.
        for fpath in self.zipfiles:
            with zipfile.ZipFile(fpath, "r") as f:
                Path(join(self.root, fpath[:-4])).mkdir(parents=True, exist_ok=True)
                f.extractall(join(self.root, fpath[:-4]))
                files.append([join(self.root, fpath[:-4], i) for i in f.namelist()])

        # siteDateRE = re.compile("\.[a-z]{3}_(.*)\.[0-9]{4}-[0-9]{2}\." + self.data["package"] + "(.*)\.csv")
        # siteAllRE = re.compile("\.[a-z]{3}_([a-z]*)\."+self.data["package"]+"(.*)\.csv")
        # expanded packages sometimes contain basic files.
        siteDateRE = re.compile(
            "\.[a-z]{3}_(.*)\.[0-9]{4}-[0-9]{2}\." + "[a-z]*" + "(.*)\.csv"
        )
        siteAllRE = re.compile("\.[a-z]{3}_([a-zA-Z]*)\.[a-z]*\.(.*)\.csv")
        labRE = re.compile("NEON\.(.*)\.[a-z]{3}_([a-zA-Z]*)\.csv")
        self.labRE = labRE

        self.siteDateFiles = [[i for i in j if siteDateRE.search(i)] for j in files]
        self.siteAllFiles = [[i for i in j if siteAllRE.search(i)] for j in files]
        self.labFiles = [
            [i for i in j if labRE.match(os.path.basename(i))] for j in files
        ]

        self.stackedFiles = {}

        # key functions
        self.stack_site_date()
        self.stack_site_all()
        self.stack_lab()

        if clean:
            # inherited
            self.cleandir(self.root)

    def stack_site_date(self):
        """stacks site-date files. requires self.siteDateFiles to have been pregenerated.
        Outputs to self.stackedFiles.
        """
        # TODO site date is not always common between sites.
        flat = set(
            [self.extractName(i) for i in list(chain.from_iterable(self.siteDateFiles))]
        )
        for name in flat:
            filename = join(self.stackedDir, name + "_stacked.csv")
            out = neon.CSVwriter(filename)
            for other in range(len(self.siteDateFiles)):
                for i in self.siteDateFiles[other]:
                    if name in i:
                        out.append(join(self.root, i))
                        break

            out.close()
            self.stackedFiles[name] = filename

    def instances(self, name, files):
        """Helper function for *-all stacking. Finds tables
        with the same name from each site.
        """

        # requires files to be presorted.
        inst = {i: None for i in self.data["site"]}
        for i in files:
            for j in i:
                if name in j:
                    inst[self.extractLoc(j)] = j
                    continue
        return inst

    def stack_site_all(self):
        """stack site-all"""

        # TODO file may not be in the latest version.
        if len(self.siteAllFiles) == 0:
            return
        for i in self.siteAllFiles[0]:
            name = self.extractName(i)
            toStack = self.instances(name, self.siteAllFiles)
            filename = join(self.stackedDir, name + "_stacked.csv")
            outf = neon.CSVwriter(filename)
            for j in toStack:
                if not toStack[j]:
                    continue
                outf.append(join(self.root, toStack[j]))

            outf.close()
            self.stackedFiles[name] = filename

    def stack_lab(self):
        if len(self.labFiles) == 0:
            return
        for i in self.labFiles[0]:
            name = self.extractName(i)
            if self.is_lab_all(name, self.extractLoc(i), self.labFiles):
                self.stack_lab_all(name)
            else:
                self.stack_lab_cur(name)

    def stack_lab_all(self, name):
        toStack = self.instances(name, self.labFiles)
        filename = join(self.stackedDir, name + "_stacked.csv")
        outf = neon.CSVwriter(filename)
        for j in toStack:
            if not toStack[j]:
                continue
            outf.append(join(self.root, toStack[j]))
        outf.close()
        self.stackedFiles[name] = filename

    def stack_lab_cur(self, name):
        flat = set([i for i in list(chain.from_iterable(self.labFiles)) if name in i])
        filename = join(self.stackedDir, name + "_stacked")
        out = neon.CSVwriter(filename)
        for f in flat:
            out.append(f)
        out.close()

    def is_lab_all(self, name, lab, files):
        # print([i for i in list(chain.from_iterable(files)) if name in i])
        hashes = [
            self.hashf(i)
            for i in list(chain.from_iterable(files))
            if name in i and lab in i
        ]
        # if len(hashes)!=len(set(hashes)):
        if 1 != len(set(hashes)):
            return True
        return False


def test():
    n = NeonObservational(
        dpID="DP1.10055.001",
        site=["WOOD", "MOAB"],
        dates={"WOOD": ["2017-05"], "MOAB": ["2017-08"]},
    )
    n.download()
    n.stackByTable()


test()
