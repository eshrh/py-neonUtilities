import json
import requests
import urllib
import utils

class zipsByProduct:
    def __init__(self,data):

        self.idxurls = self.constructIdxUrls(data)
        index = json.loads(requests.get(self.idxurls[0]).text)['data']['files']
        print(index[0]['name'])

    def constructIdxUrls(self,data):
        urls = []
        if data['site']==str:
            data['site'] = [data['site']]
        if data['edate']== "":
           data['edate'] = data['sdate']
        dates = utils.getRangeDates(data['sdate'],data['edate'])

        for site in data['site']:
            urls.extend([utils.basicUrl(data['dpID'],site,date) for date in dates])

        return urls




data = {"dpID":"DP1.10003.001","site":["WOOD"],"sdate":"2015-07","edate":"2015-07"}
obj = zipsByProduct(data)
