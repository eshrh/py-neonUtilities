from neonUtilities.NeonInstrumental import NeonInstrumental
wq = NeonInstrumental(dpID="DP1.20288.001", 
                              site=["BARC"],
                              avg="instantaneous",
                              package="expanded",
                              dates=[["2017-8","2017-9"]])

wq.download()
wq.stackByTable(clean=False)
print(wq.to_pandas())
