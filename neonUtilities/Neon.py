import json
import os
from utils import *
import re
import time
import requests
import urllib

class Neon:
    def __init__(self,dpID=None,site=None,dates=None,package="basic",token=None):
        self.data = {"dpID":dpID,"site":site,"dates":dates,"package":package,"token":token}

    def download(self):
        self.rootname = self.data["dpID"]
        if not os.path.exists(self.rootname):
            os.makedirs(self.rootname)
            print(f"[created root folder {self.rootname}]")

        self.idxurls = self.constructIdxUrls()
        print(f"{len(self.idxurls)} files in total")
        self.zipfiles = []
        for n,idxurl in enumerate(self.idxurls):
            self.currentlyDl = n
            print(f"Downloading zip {n+1}")
            self.downloadZips(idxurl)
        print("Done downloading.")

    def makeReq(self,url):
        if data['token']:
            return req.get(url,headers={"X-API-TOKEN":self.data['token']})
        else:
            return req.get(url)

    def downloadZips(self,idxurl):
        req = self.makeReq(idxurl)
        while req.headers["X-RateLimit-Remaining"]==0:
            print("Rate limit exceeded. Consider using an api token. Pausing...")
            time.sleep(req.headers['RetryAfter'])
            print("Retrying")
            req = self.makeReq(idxurl)

        zipidx = None
        for i in range(len(index)):
            match = zipre.match(index[i]['name'])
            if match:
                zipidx = i
                break
        if zipidx:
            urllib.request.urlretrieve(index[zipidx]['url'],os.path.join(self.rootname, index[zipidx]['name']))
            self.zipfiles.append(os.path.join(os.getcwd(),self.rootname,index[zipidx]['name']))
            print(f"Completed zip {self.currentlyDl+1}")
        else:
            print(f"Zip file missing. This may be because this data chunk {idxurl} does not exist.")

    def constructIdxUrls(self):
        urls = []
        if self.data['site']==str:
            self.data['site'] = [self.data['site']]

        dates = []
        for date in self.data["dates"]:
            if type(date)==str:
                dates.append(date)
            else:
                dates.extend(getRangeDates(date[0],date[1]))

        for site in self.data['site']:
            urls.extend([basicUrl(self.data['dpID'],site,date) for date in dates])
        return urls

