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

        self.root = (
            join(os.getcwd(), self.rootname)
            if not root
            else join(os.getcwd(), root)
        )

        self.stackedDir = join(os.getcwd(), root, "stackedFiles")

        if os.path.exists(self.stackedDir):
            shutil.rmtree(self.stackedDir)
        os.makedirs(self.stackedDir)

        files = []
        self.filepaths = {}
        self.zipfiles = sorted(self.zipfiles)

        for fpath in self.zipfiles:
            with zipfile.ZipFile(fpath, "r") as f:
                Path(join(self.root,fpath[:-4])).mkdir(parents=True, exist_ok=True)
                f.extractall(join(self.root,fpath[:-4]))
                namelist = f.namelist()
                files.append(namelist)
                for i in namelist:
                    self.filepaths[i] = join(self.root,fpath[:-4],i)

        # siteDateRE = re.compile("\.[a-z]{3}_(.*)\.[0-9]{4}-[0-9]{2}\." + self.data["package"] + "(.*)\.csv")
        # siteAllRE = re.compile("\.[a-z]{3}_([a-z]*)\."+self.data["package"]+"(.*)\.csv")
        # expanded packages sometimes contain basic files.
        siteDateRE = re.compile(
            "\.[a-z]{3}_(.*)\.[0-9]{4}-[0-9]{2}\." + "[a-z]*" + "(.*)\.csv"
        )

        siteAllRE = re.compile("\.[a-z]{3}_([a-zA-Z]*)\.[a-z]*\.(.*)\.csv")

        self.siteDateFiles = [[i for i in j if siteDateRE.search(i)] for j in files]
        self.siteAllFiles = [[i for i in j if siteAllRE.search(i)] for j in files]

        self.stackedFiles = {}
        self.stack_site_date()
        self.stack_site_all()

        if clean:
            # inherited
            self.cleandir(self.root)

    def stack_site_date(self):
        # TODO site date is not always common between sites.
        flat = set([self.extractName(i) for i in list(chain.from_iterable(self.siteDateFiles))])
        for name in flat:
            filename = join(self.stackedDir, name + "_stacked.csv")
            out = neon.CSVwriter(filename)
            for other in range(len(self.siteDateFiles)):
                for i in self.siteDateFiles[other]:
                    if name in i:
                        path = self.filepaths[i]
                        out.append(join(self.root,path))
                        break

            out.close()
            self.stackedFiles[name] = filename

    def instances(self, name, files):
        # requires files to be presorted.
        inst = {i: None for i in self.data["site"]}
        for i in files:
            for j in i:
                if name in j:
                    inst[self.extractSite(j)] = j
                    continue
        return inst

    def stack_site_all(self):
        #TODO file may not be in the latest version.
        if len(self.siteAllFiles) == 0:
            return
        for i in self.siteAllFiles[0]:
            name = self.extractName(i)
            toStack = self.instances(name, self.siteAllFiles)
            filename = join(self.stackedDir, name + "_stacked.csv")
            outf = neon.CSVwriter(filename)
            for j in toStack:
                outf.append(join(self.root, self.filepaths[toStack[j]]))
            outf.close()
            self.stackedFiles[name] = filename

    def to_pandas(self):
        """Converts a stacked dataset into a dictionary of pandas DataFrames"""
        if len(self.stackedFiles) == 0:
            print("No files stacked")
            return
        import pandas as pd

        dfs = {}
        for i in self.stackedFiles:
            dfs[i] = pd.read_csv(self.stackedFiles[i])
        return dfs


# tester function to remove when publishing on pypi

def test():
    obj = NeonObservational(
        dpID="DP1.10104.001",
        site=["NIWO"],
        #dates=[["2019-06","2019-09"]],
        dates=[["2019-06","2019-07"]],
        package="expanded",
    )
    obj.download()
    obj.stackByTable(clean=True)
    df= obj.to_pandas()

def test2():
    obj = NeonObservational(
            dpID="DP1.20138.001",
            # TODO Check lab-all and lab-current.
            # TODO copy latest version of _all_ files.
            site=["REDB","PRIN"],
            dates=["2020-02"],
            package="expanded",
        )
    obj.download()
    obj.stackByTable(clean=True)
    df = obj.to_pandas()

    print(df["amc_fieldCellCounts"])

test2()
