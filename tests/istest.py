from neonUtilities.NeonInstrumental import NeonInstrumental

n = NeonInstrumental(dpID="DP1.00003.001",site="MOAB",dates=["2018-05","2018-06"],avg=30)
n.download()
n.stackByTable(root="DP1.00003.001")

