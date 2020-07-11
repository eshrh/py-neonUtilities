from neonUtilities.NeonObservational import NeonObservational

obj = NeonObservational(
    dpID="DP1.20138.001",
    # TODO Check lab-all and lab-current.
    # TODO copy latest version of _all_ files.
    site=["REDB", "PRIN"],
    dates=["2020-02"],
    package="expanded",
)
obj.download()
obj.stackByTable(clean=False)
df = obj.to_pandas()

print(df["amc_fieldCellCounts"])
