baseurl = "https://data.neonscience.org/api/v0/data/"

def mkdt(y,m):
    if m<10:
        m = "0"+str(m)
    return str(y)+"-"+str(m)
def getRangeDates(sdate,edate):
    dates = []
    sy,sm = int(sdate.split("-")[0]), int(sdate.split("-")[1])
    ey,em = int(edate.split("-")[0]), int(edate.split("-")[1])
    if ey==sy:
        dates.extend([mkdt(sy,i) for i in range(sm,em+1)])
    else:
        dates.extend([mkdt(sy,i) for i in range(sm,13)])
        for i in range(sy+1,ey):
            dates.extend([mkdt(i,j) for j in range(1,13)])
        dates.extend([mkdt(ey,i) for i in range(1,em+1)])
    return dates

def basicUrl(dpid,site,date):
    return baseurl+dpid+"/"+site+"/"+date


print(basicUrl("sdf","sdf","sjdf"))
