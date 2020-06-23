import json
import requests
import urllib
from utils import *
import os
import re

class NeonObservational:
    def __init__(self,dpID=None,site=None,sdate=None,edate=None,package="basic",token=None):
        self.data = {"dpID":dpID,"site":site,"sdate":sdate,"edate":edate,"package":package,"token":token}

    def download(self):
        self.rootname = data["dpID"]
        if not os.path.exists(self.rootname):
            os.makedirs(self.rootname)
            print(f"[created root folder {self.rootname}]")
        self.idxurls = self.constructIdxUrls()
        print(f"{len(self.idxurls)} files in total")
        self.zipfiles = []
        for n,idxurl in enumerate(self.idxurls):
            print(f"Downloading zip {n+1}")
            self.downloadZips(idxurl)
        print("Done downloading.")

    def downloadZips(self,idxurl):
        req = requests.get(idxurl)
        while req.headers["X-RateLimit-Remaining"]==0:
            print("Rate limit exceeded. Consider using an api token. Pausing...")
            time.sleep(req.headers['RetryAfter'])
            print("Retrying")
            req = requests.get(idxurl)
           
        index = json.loads(req.text)['data']['files']
        zipre = re.compile("(.*)"+self.data["package"]+"(.*)zip")
        for i in range(len(index)):
            match = zipre.match(index[i]['name'])
            if match:
                zipidx = i
                break
        urllib.request.urlretrieve(index[zipidx]['url'],os.path.join(self.rootname, index[zipidx]['name']))
        self.zipfiles.append(os.path.join(os.getcwd(),self.rootname,index[zipidx]['name']))


    def constructIdxUrls(self):
        urls = []
        if self.data['site']==str:
            self.data['site'] = [self.data['site']]
        if self.data['edate']== "":
           self.data['edate'] = self.data['sdate']
        dates = getRangeDates(self.data['sdate'],self.data['edate'])
        for site in self.data['site']:
            urls.extend([basicUrl(self.data['dpID'],site,date) for date in dates])
        return urls

def test():
    data = {"dpID":"DP1.10003.001","site":["WOOD"],"sdate":"2015-07","edate":"2015-07","package":"basic"}
    obj = NeonObservational(data)
