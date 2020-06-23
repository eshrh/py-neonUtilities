import glob
from Neon import Neon
import os
import zipfile
import shutil as sh
import re


class NeonObservational(Neon):
    def __init__(self,dpID=None,site=None,dates=None,package="basic",token=None):
        Neon.__init__(self,dpID,site,dates,package,token)
        self.nameRE = re.compile("[0-9]{3}\.(.*)\.[0-9]{4}-[0-9]{2}\.")

    def extractName(self,s):
        match = self.nameRE.search(s)
        if not match:
            return None
        return s[match.start()+8:match.end()-9]

    def stackByTable(self,root=None):
        #another solution may be to read the EML file. This is more robust,
        #but can be extremely slow with extraneous information.

        if not root:
            root = os.path.join(os.getcwd(),self.rootname)
        else:
            root = os.path.join(os.getcwd(),root)
            self.zipfiles = glob.glob(os.path.join(root,"*.zip"))

        root = os.path.join(os.getcwd(),self.rootname) if not root else os.path.join(os.getcwd(),root)
        stackedDir = os.path.join(os.getcwd(),root,"stackedFiles")

        if not os.path.exists(stackedDir):
            os.makedirs(stackedDir)
        files = []
        for fpath in self.zipfiles:
            with zipfile.ZipFile(fpath,'r') as f:
                f.extractall(root)
                files.append(f.namelist())

        dataFileRE = re.compile("(.*)\.[0-9]{4}-[0-9]{2}\."+self.data['package']+"(.*)\.csv")
        files = [[i for i in j if dataFileRE.search(i)] for j in files]
        for i in files[0]:
            name = self.extractName(i)
            out = open(os.path.join(stackedDir,name+"_stacked"),"a")
            for line in open(os.path.join(root,i)):
                out.write(line)

            for other in range(1,len(files)):
                with open(os.path.join(root,[i for i in files[other] if name in i][0])) as otherf:
                    otherf.__next__()
                    for line in otherf:
                        out.write(line)

            out.close()

def test():
    obj = NeonObservational(dpID="DP1.10003.001", site=["WOOD"], dates=["2015-07","2017-07"], package="basic")
    obj.download()
    #obj.stackByTable(obj.data["dpID"])
    obj.stackByTable()


test()
