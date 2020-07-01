import glob

from importlib import reload
import neon

reload(neon)

import os
import shutil
import zipfile
import re


class NeonObservational(neon.Neon):
    def __init__(self, dpID=None, site=None, dates=None, package="basic", token=None):
        neon.Neon.__init__(self, dpID, site, dates, package, token)
        #self.nameRE = re.compile("[0-9]{3}\.(.*)\.([0-9]{4}-[0-9]{2}|"+package+")\.")
        self.nameRE = re.compile("[0-9]{3}\.(.*)\.([0-9]{4}-[0-9]{2}|[a-z]*)\.")
        self.stackedFiles = {}

    def extractName(self, s):
        match = self.nameRE.search(s)
        if not match:
            return None

        matchstr = str(match.group(0))
        if matchstr.count(".")==4:
            return s[match.start() + 8 : match.end()-7]
        if matchstr.count(".")==5:
            return s[match.start() + 8 : match.end()-15]

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
            root = os.path.join(os.getcwd(), self.rootname)
        else:
            root = os.path.join(os.getcwd(), root)
            self.zipfiles = glob.glob(os.path.join(root, "*.zip"))

        if len(self.zipfiles)==0:
            print("No download files found. Pass the download directory to this function or use the download() method.")
            return

        self.root = (
            os.path.join(os.getcwd(), self.rootname)
            if not root
            else os.path.join(os.getcwd(), root)
        )

        self.stackedDir = os.path.join(os.getcwd(), root, "stackedFiles")

        if os.path.exists(self.stackedDir):
            shutil.rmtree(self.stackedDir)
        os.makedirs(self.stackedDir)


        files = []
        self.zipfiles = sorted(self.zipfiles)

        for fpath in self.zipfiles:
            with zipfile.ZipFile(fpath, "r") as f:
                f.extractall(self.root)
                files.append(f.namelist())

        #siteDateRE = re.compile("\.[a-z]{3}_(.*)\.[0-9]{4}-[0-9]{2}\." + self.data["package"] + "(.*)\.csv")
        #siteAllRE = re.compile("\.[a-z]{3}_([a-z]*)\."+self.data["package"]+"(.*)\.csv")
        #expanded packages sometimes contain basic files.
        siteDateRE = re.compile("\.[a-z]{3}_(.*)\.[0-9]{4}-[0-9]{2}\." + "[a-z]*" + "(.*)\.csv")

        siteAllRE = re.compile("\.[a-z]{3}_([a-zA-Z]*)\.[a-z]*\.(.*)\.csv")

        self.siteDateFiles = [[i for i in j if siteDateRE.search(i)] for j in files]
        self.siteAllFiles = [[i for i in j if siteAllRE.search(i)] for j in files]

        self.stackedFiles = {}
        self.stack_site_date()
        self.stack_site_all()

        if clean:
            #inherited
            self.cleandir(self.root)

    def stack_site_date(self):
        for i in self.siteDateFiles[0]:
            name = self.extractName(i)
            filename = os.path.join(self.stackedDir, name + "_stacked.csv")
            out = open(filename, "a")
            for line in open(os.path.join(self.root, i)):
                out.write(line)

            for other in range(1, len(self.siteDateFiles)):
                with open(os.path.join(self.root, [i for i in self.siteDateFiles[other] if name in i][0]),"r") as otherf:
                    otherf.__next__()
                    for line in otherf:
                        out.write(line)

            out.close()
            self.stackedFiles[name] = filename

    def stack_site_all(self):
        if len(self.siteAllFiles)==0:
            return
        for i in self.siteAllFiles[0]:
            name = self.extractName(i)
            outf = os.path.join(self.stackedDir, name+"_stacked.csv")
            shutil.copyfile(os.path.join(self.root,i),outf)
            self.stackedFiles[name] = outf

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
        dpID="DP1.10055.001",
        #TODO Check lab-all and lab-current.
        #TODO copy latest version of _all_ files.
        #TODO how to deal with multiple site metadata???

        site=["BART","JORN"],
        dates=["2017-03"],
        package="basic",
    )
    #obj.download()
    obj.stackByTable("DP1.10055.001",clean=False)


test()

