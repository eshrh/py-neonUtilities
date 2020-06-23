import json
import requests
import urllib
import utils
import os
import re

class NeonObservational:
    def __init__(self,data):
        self.data = data

        rootname = data["dpID"]
        if not os.path.exists(rootname):
            os.makedirs(rootname)
        print(f"[created root folder {rootname}]")

        self.idxurls = self.constructIdxUrls()
        print(f"{len(self.idxurls)} files in total")
        for n,idxurl in enumerate(self.idxurls):
            print(f"Downloading zip {n+1}")
            self.downloadZips(idxurl)


    def downloadZips(self,idxurl):
        index = json.loads(requests.get(idxurl).text)['data']['files']
        zipre = re.compile("(.*)"+data["package"]+"(.*)zip")
        for i in range(len(index)):
            match = zipre.match(index[i]['name'])
            if match:
                break
        print(match)
        zipname = index[zipidx]['name']



    def constructIdxUrls(self):
        urls = []
        if self.data['site']==str:
            self.data['site'] = [self.data['site']]
        if self.data['edate']== "":
           self.data['edate'] = self.data['sdate']
        dates = utils.getRangeDates(self.data['sdate'],self.data['edate'])
        for site in self.data['site']:
            urls.extend([utils.basicUrl(self.data['dpID'],site,date) for date in dates])
        return urls




data = {"dpID":"DP1.10003.001","site":["WOOD"],"sdate":"2015-07","edate":"2015-07","package":"basic"}
obj = NeonObservational(data)
