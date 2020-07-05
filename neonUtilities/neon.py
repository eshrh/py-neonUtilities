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

    def __init__(self, dpID=None, site=None, dates=None, package="basic", token=None):
        if package != "basic" and package != "expanded":
            print("package must be either basic or expanded. defaulting to basic.")
            package = "basic"

        self.data = {
            "dpID": dpID,
            "site": site,
            "dates": dates,
            "package": package,
            "token": token,
        }
        self.baseurl = "https://data.neonscience.org/api/v0/data/"
        self.zipre = re.compile("(.*)" + self.data["package"] + "(.*)zip")
        self.nameRE = re.compile(
            "NEON\.(.*)\.[a-z]{3}_([a-zA-Z]*)\.csv|[0-9]{3}\.(.*)\.([0-9]{4}-[0-9]{2}|[a-z]*)\."
        )

    def download(self):
        """Class method to download zip files"""

        self.rootname = self.data["dpID"]

        # create dataproduct directory if it does not exist
        # TODO allow for multiple dataproducts to be downloaded at once and stacked.
        if not os.path.exists(self.rootname):
            os.makedirs(self.rootname)
            print(f"[created root folder {self.rootname}]")

        # find all the idxurls of month-chunks
        self.idxurls = self.constructIdxUrls()
        print(f"{len(self.idxurls)} files in total")
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

    def downloadZips(self, idxurl):
        """privately used function to download an individual zip file from url"""

        req = self.makeReq(idxurl)
        while req.headers["X-RateLimit-Remaining"] == 0:
            print("Rate limit exceeded. Consider using an api token. Pausing...")
            time.sleep(req.headers["RetryAfter"])
            print("Retrying")
            req = self.makeReq(idxurl)

        index = json.loads(req.text)["data"]["files"]

        zipidx = None
        for i in range(len(index)):
            match = self.zipre.match(index[i]["name"])
            if match:
                zipidx = i
                break
        if zipidx:
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

    def mkdt(self, y, m):
        if m < 10:
            m = "0" + str(m)
        return str(y) + "-" + str(m)

    def getRangeDates(self, sdate, edate):
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
        if self.data["site"] == str:
            self.data["site"] = [self.data["site"]]

        dates = []
        for date in self.data["dates"]:
            # if it is an individual date, mark it for download
            # else, get the range of dates specified in the inner iterable.
            if type(date) == str:
                dates.append(date)
            else:
                dates.extend(self.getRangeDates(date[0], date[1]))

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

    def hashf(self,filename):
        md5_hash = hashlib.md5()
        with open(filename,"rb") as f:
            # Read and update hash in chunks of 4K
            for byte_block in iter(lambda: f.read(4096),b""):
                md5_hash.update(byte_block)
        return md5_hash.hexdigest()


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
