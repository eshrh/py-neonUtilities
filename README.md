# Py-NEONutilities

This project is a heavy work in progress to write a native python wrapper for the [NEON data platform](https://data.neonscience.org).
It aims to provide the functionality of the [NEONutilities R package](https://github.com/NEONScience/NEON-utilities/tree/master/neonUtilities)
without the overhead of having to manage `rpy`.

## Progress
- [X] Observational data
  - [X] Download data zips 
  - [X] Table stacking
  - [ ] Support for READMEs, EML and variables files
- [ ] Instrumentals
- [ ] AOP/EC data
- [X] package for pypi (check the pypi branch)

You can install this package from pypi with `pip install py-neonUtils`

The `neonUtilities.NeonObservational.NeonObservational` class contains the primary utilities to download and stack data.

An example of general usage:
```python
from neonUtilities.NeonObservational import NeonObservational
import pandas as pd

neonobj = NeonObservational(dpID="DP1.10003.001", site=["WOOD"], dates=["2015-07","2017-07"], package="basic")
#Download two specific basic month-chunks from the WOOD site for DP1.10003.001

neonobj.download()
neonobj.stackByTable()
df = neonobj.to_pandas()
```

This modules contains some improvements over
the original R module. You can pass a 2-dimensional array to `dates` to download several ranges of month-chunk data.
You can also pass the zip directory name to `stackByTable` to use an existing download similarly to the R package.

 

