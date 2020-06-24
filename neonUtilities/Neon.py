import json
import os
from utils import *
import re
import time
import requests
import urllib


class Neon:
    """Parent class for all Neon datatypes"""

    def __init__(self, dpID=None, site=None, dates=None, package="basic", token=None):
        self.data = {
            "dpID": dpID,
            "site": site,
            "dates": dates,
            "package": package,
            "token": token,
        }

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

        if data["token"]:
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

        zipidx = None
        for i in range(len(index)):
            match = zipre.match(index[i]["name"])
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
                dates.extend(getRangeDates(date[0], date[1]))

        for site in self.data["site"]:
            urls.extend([basicUrl(self.data["dpID"], site, date) for date in dates])
        return urls
