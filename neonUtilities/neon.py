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

#TODO implement file-size summation.

import json
import os
import re
import time
import requests as req
import urllib
import glob
import shutil
import hashlib


class Neon:
    """Parent class for all Neon datatypes"""

    def __init__(
        self, dpID=None, site=None, dates=None, avg=None, package="basic", token=None
    ):
        if package != "basic" and package != "expanded":
            print("package must be either basic or expanded. defaulting to basic.")
            package = "basic"

        self.data = {
            "dpID": dpID,
            "site": site,
            "dates": dates,
            "avg": avg,
            "package": package,
            "token": token,
        }

        self.baseurl = "https://data.neonscience.org/api/v0/data/"
        self.zipre = re.compile(package+"(.*)\.zip")
        self.nameRE = re.compile(
            "NEON\.(.*)\.[a-z]{3}_([a-zA-Z]*)\.csv|[0-9]{3}\.(.*)\.([0-9]{4}-[0-9]{2}|[a-z]*)\."
        )
        self.filere = re.compile(".csv|.xml")
        self.packagere = re.compile(package)
        self.folders = []

    def makeIfNotExists(self, name):
        if not os.path.exists(name):
            os.makedirs(name)
            return True
        return False

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
            print(f"Downloading zip {n+1}")
            self.downloadZips(idxurl)
        print("Done downloading.")

    def makeReq(self, url):
        """wrapper function to allow for API token requests"""
        if self.data["token"]:
            return req.get(url, headers={"X-API-TOKEN": self.data["token"]})
        else:
            return req.get(url)

    def getReq(self, idxurl):
        """catch ratelimit exceeded and wait"""
        req = self.makeReq(idxurl)
        while req.headers["X-RateLimit-Remaining"] == 0:
            print("Rate limit exceeded. Consider using an api token. Pausing...")
            time.sleep(req.headers["RetryAfter"])
            print("Retrying")
            req = self.makeReq(idxurl)

        return json.loads(req.text)["data"]["files"]

    def downloadZips(self, idxurl):
        """privately used function to download an individual zip file from url"""
        index = self.getReq(idxurl)
        zipidx = None
        for i in range(len(index)):
            match = self.zipre.search(index[i]["name"])
            if match:
                zipidx = i
                break
        if zipidx!=None:
            urllib.request.urlretrieve(
                index[zipidx]["url"], os.path.join(self.rootname, index[zipidx]["name"])
            )
            self.zipfiles.append(
                os.path.join(os.getcwd(), self.rootname, index[zipidx]["name"])
            )
            print(f"Completed zip {self.currentlyDl+1}")
        else:
            print(
                f"Zip file missing. This may be because this data chunk {idxurl} does not exist."
            )

    def downloadFiles(self, idxurl, re=None):
        """Downloads files instead of zips"""
        index = self.getReq(idxurl)
        foldername = None
        for i in index:
            if self.zipre.match(i["name"]):
                foldername = i["name"][:-4]
                break

        if not foldername:
            print(
                "File missing. This may be because data chunk {idxurl} does not exist."
            )
            return

        self.makeIfNotExists(os.path.join(self.rootname, foldername))
        if not re:
            re = self.filere

        for i in index:
            if re.search(i["name"]) and self.packagere.search(i["name"]):
                urllib.request.urlretrieve(
                    i["url"], os.path.join(self.rootname, foldername, i["name"])
                )

        self.folders.append(foldername)

    def mkdt(self, y, m):
        if m < 10:
            m = "0" + str(m)
        return str(y) + "-" + str(m)

    def getRangeDates(self, pair):
        if type(pair)==str:
            return [pair]
        sdate,edate = pair
        dates = []
        sy, sm = int(sdate.split("-")[0]), int(sdate.split("-")[1])
        ey, em = int(edate.split("-")[0]), int(edate.split("-")[1])
        if ey == sy:
            dates.extend([self.mkdt(sy, i) for i in range(sm, em + 1)])
        else:
            dates.extend([self.mkdt(sy, i) for i in range(sm, 13)])
            for i in range(sy + 1, ey):
                dates.extend([self.mkdt(i, j) for j in range(1, 13)])
            dates.extend([self.mkdt(ey, i) for i in range(1, em + 1)])
        return dates

    def basicUrl(self, dpid, site, date):
        return self.baseurl + dpid + "/" + site + "/" + date

    def constructIdxUrls(self):
        urls = []
        # if there is only one site as a string, turn it into a single element array.
        if type(self.data["site"]) == str:
            self.data["site"] = [self.data["site"]]

        urls = []
        if type(self.data["dates"]) == dict:
            for i in self.data["dates"]:
                dates = []
                for date in self.data["dates"][i]:
                    dates.extend(self.getRangeDates(date))
                urls.extend([self.basicUrl(self.data["dpID"],i,date) for date in dates])
        else:
            dates = []
            for date in self.data["dates"]:
                dates.extend(self.getRangeDates(date))

            for site in self.data["site"]:
                urls.extend(
                    [self.basicUrl(self.data["dpID"], site, date) for date in dates]
                )

        return urls

    def cleandir(self, direc):
        toRemove = [
            i
            for i in glob.glob(os.path.join(direc, "*"))
            if not self.zipre.match(i) and "stackedFiles" not in i
        ]
        for i in toRemove:
            if os.path.isdir(i):
                shutil.rmtree(i)
            else:
                os.remove(i)

    def extractName(self, s):
        s = os.path.basename(s)
        match = self.nameRE.search(s)
        if not match:
            return None
        matchstr = str(match.group(0))
        if matchstr.count(".") == 3:
            return matchstr.split(".")[-2]
        if matchstr.count(".") == 4:
            return s[match.start() + 8 : match.end() - 7]
        if matchstr.count(".") == 5:
            return s[match.start() + 8 : match.end() - 15]

    def extractLoc(self, s):
        s = os.path.basename(s)
        if self.labRE.match(s):
            return s.split(".")[1]

        return os.path.basename(s).split(".")[2]

    def hashf(self, filename):
        md5_hash = hashlib.md5()
        with open(filename, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
        return md5_hash.hexdigest()

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


class CSVwriter:
    def __init__(self, outname):
        self.filename = outname
        self.out = open(self.filename, "a")
        self.first = True

    def append(self, inname):
        with open(inname) as otherf:
            if not self.first:
                otherf.__next__()
            for line in otherf:
                self.out.write(line)
            self.first = False

    def close(self):
        self.out.close()
