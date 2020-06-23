# Py-NEONutilities

This project is a heavy work in progress to write a native python wrapper for the [NEON data platform](https://data.neonscience.org).
It aims to provide the functionality of the [NEONutilities R package](https://github.com/NEONScience/NEON-utilities/tree/master/neonUtilities)
without the overhead of having to manage `rpy`.

## Progress
- [X] Observational data
  - [X] Download data zips 
  - [X] Table stacking
  - [ ] Support for READMEs, EML and variables files
- [ ] AOP/EC data
- [ ] package for pypi

You can install this package from pypi with `pip install py-neonUtils`

The `neonUtilities.NeonObservational.NeonObservational` class contains the primary utlities to download and stack data.

An example of general usage:
```python
from neonUtilities.NeonObservational import NeonObservational

neonobj = NeonObservational(dpID="DP1.10003.001", site=["WOOD"], dates=["2015-07","2017-07"], package="basic")
neonobj.download()
neonobj.stackByTable()
```

`NeonObservational` supports multiple sites data, as well as passing a 2-dimensional array of ranges to `dates` to download several ranges of month-chunk data.

 

