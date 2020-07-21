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

# TODO make sure that you can actually only provide dates and not site.

import json
import os
from os.path import join
import re
import time
import requests as req
import urllib
import glob
import shutil
import zlib
import math


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
        self.zipre = re.compile(package + "\.(.*)\.zip")
        self.nameRE = re.compile(
            "NEON\.(.*)\.[a-z]{3}_([a-zA-Z]*)\.csv|[0-9]{3}\.(.*)\.([0-9]{4}-[0-9]{2}|[a-z]*)\."
        )

        if avg != None:
            if type(avg) == int:
                avg = str(avg)
                self.isre = re.compile(
                    "[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.(.*)_" + avg + "min"
                )
            else:
                avg = str(avg)
                self.isre = re.compile(
                    "[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.(.*)_" + avg
                )
        else:
            self.isre = re.compile("[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.[0-9]{3}\.(.*)_(.*)")

        self.filere = re.compile(".csv")
        self.packagere = re.compile(package)
        self.folders = []
        self.zipfiles = []

    def makeIfNotExists(self, name):
        if not os.path.exists(name):
            os.makedirs(name)
            return True
        return False

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

        res = False
        for n, idxurl in enumerate(self.idxurls):
            self.currentlyDl = n
            if self.data["avg"] != None:
                res = self.downloadFiles(idxurl, usere=self.isre)
                # TODO hashchecking
                # TODO sizechecking
            else:
                res = self.downloadZips(idxurl)
        if res:
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
        while req.headers["X-RateLimit-Remaining"] == 1:
            print("Rate limit exceeded. Consider using an api token. Pausing...")
            time.sleep(req.headers["RetryAfter"])
            print("Retrying")
            req = self.makeReq(idxurl)

        return json.loads(req.text)["data"]["files"]

    def readable(self, size):
        if size == 0:
            return "0"
        exts = ("B", "KB", "MB", "GB")
        ext = int(math.floor(math.log(size, 1024)))
        val = round(size / math.pow(1024, ext), 2)
        return f"{val} {exts[ext]}"

    def downloadZips(self, idxurl):
        """privately used function to download an individual zip file from url"""
        index = self.getReq(idxurl)

        zipidx = None
        for i in range(len(index)):
            match = self.zipre.search(index[i]["name"])
            if match:
                zipidx = i
                break
        if zipidx != None:
            size = int(index[zipidx]["size"])
            print(
                f"Downloading chunk {self.currentlyDl+1}. Size: {self.readable(size)}"
            )
            try:
                urllib.request.urlretrieve(
                    index[zipidx]["url"], os.path.join(self.rootname, index[zipidx]["name"])
                )
            except:
                print("Download failed, continuing...")

            self.zipfiles.append(
                os.path.join(os.getcwd(), self.rootname, index[zipidx]["name"])
            )
            print(f"Completed zip {self.currentlyDl+1}")
        else:
            print(
                f"Zip file missing. This may be because this data chunk {idxurl} does not exist."
            )
            return False
        return True

    def downloadFiles(self, idxurl, usere=None):
        """Downloads files instead of zips"""
        index = self.getReq(idxurl)
        foldername = None
        for i in index:
            if self.zipre.search(i["name"]):
                foldername = i["name"][:-4]
                break

        if not foldername:
            print(
                f"File missing. This may be because data chunk {idxurl} does not exist."
            )
            return False
        self.makeIfNotExists(os.path.join(self.rootname, foldername))

        if not usere:
            usere = self.filere

        print(f"Downloading chunk {self.currentlyDl+1}.")
        for i in index:
            if usere.search(i["name"]) and self.packagere.search(i["name"]):
                urllib.request.urlretrieve(
                    i["url"], os.path.join(self.rootname, foldername, i["name"])
                )
        self.folders.append(foldername)
        print(f"Completed chunk {self.currentlyDl+1}")
        return True

    def mkdt(self, y, m):
        if m < 10:
            m = "0" + str(m)
        return str(y) + "-" + str(m)

    def getRangeDates(self, pair):
        if type(pair) == str:
            return [pair]
        sdate, edate = pair
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
        # if there is only one site as a string, turn it into a single element array.
        if type(self.data["site"]) == str:
            self.data["site"] = [self.data["site"]]

        urls = []
        if type(self.data["dates"]) == dict:
            for i in self.data["dates"]:
                dates = []
                for date in self.data["dates"][i]:
                    dates.extend(self.getRangeDates(date))
                urls.extend(
                    [self.basicUrl(self.data["dpID"], i, date) for date in dates]
                )
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
            if not self.zipre.search(i) and "stackedFiles" not in i
        ]
        for i in toRemove:
            if os.path.isdir(i):
                shutil.rmtree(i)
            else:
                os.remove(i)

    def resetDir(self, direc):
        if os.path.exists(direc):
            shutil.rmtree(direc)
        os.makedirs(direc)

    def hashf(self, filename):
        with open(filename, "rb") as fh:
            hash = 0
            while True:
                s = fh.read(65536)
                if not s:
                    break
                hash = zlib.crc32(s, hash)
        return "%08X" % (hash)

    def to_pandas(self):
        """Converts a stacked dataset into a dictionary of pandas DataFrames"""
        if len(self.stackedFiles) == 0:
            print("No files stacked")
            return
        import pandas as pd

        dfs = {}
        for i in self.stackedFiles:
            if type(self.stackedFiles[i])==str:
                dfs[i] = pd.read_csv(self.stackedFiles[i])
            else:
                for n,name in enumerate(self.stackedFiles[i]):
                    if i in dfs:
                        dfs[i][name] = filename
                    else:
                        dfs[i] = {name:pd.read_csv(self.stackedFiles[i][name])}

        return dfs

    def stack_site_date(self, files, flatnames, site=None):
        for name in flatnames:
            if not name:
                continue
            if site is None:
                filename = join(self.stackedDir, name + "_stacked.csv")
            else:
                filename = join(self.stackedDir, site+name+"_stacked.csv")

            out = CSVwriter(filename)
            for other in range(len(files)):
                for i in files[other]:
                    if (name in i) and (site is None or site in i):
                        out.append(join(self.root, i))
                        break
            out.close()
            if site is None:
                self.stackedFiles[name] = filename
            else:
                if site in self.stackedFiles:
                    self.stackedFiles[site][name] = filename
                else:
                    self.stackedFiles[site] = {name:filename}


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
