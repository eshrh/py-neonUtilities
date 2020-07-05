from neonUtilities.NeonObservational import NeonObservational

obj = NeonObservational(
        dpID="DP1.10104.001",
        site=["NIWO"],
        dates=[["2019-06","2019-09"]],
        package="expanded",
    )
obj.download()
obj.stackByTable(clean=False)
df = obj.to_pandas()

print(df[list(df)[0]])
