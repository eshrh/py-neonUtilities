# Py-NEONutilities

This project is a heavy work in progress to write a native python wrapper for the [NEON data platform](https://data.neonscience.org).
It aims to provide the functionality of the [NEONutilities R package](https://github.com/NEONScience/NEON-utilities/tree/master/neonUtilities)
without the overhead of having to manage `rpy`.

## Installation
You can install this package from pypi with `pip install py-neonUtils`

Please see the pypi branch for downloads.

## Usage:

### Observational Data
```python
from neonUtilities.NeonObservational import NeonObservational
import pandas as pd

neonobj = NeonObservational(dpID="DP1.10003.001", site=["WOOD"], dates=["2015-07",["2017-07","2017-12"]], package="basic")
#Download data from 2015-07 and the range of 2017-07 to 2017-12.

neonobj.download()
neonobj.stackByTable()
df = neonobj.to_pandas()
```
### Instrumental Data 
```python
from neonUtilities.NeonObservational import NeonObservational
import pandas as pd

neonobj = NeonInstrumental(dpID="DP1.10003.001", site=["WOOD"], dates=["2015-07",["2017-07","2017-12"]], avg=30, package="basic")

neonobj.download()
neonobj.stackByTable()
df = neonobj.to_pandas()
```

This module offers a lot of flexibility with month-chunk downloads. You can provide:
1. A single string to download that chunk for all sites
2. A list of strings to download those chunks for all sites
3. Either a nested list or a nested tuple of dates to download that range inclusive,inclusive
4. Any combination of 2 and 3
5. A dictionary with sites as keys and number any date definition above to download different month chunks for different sites.



## Progress
- [X] Observational data
  - [X] Download data zips 
  - [X] Table stacking
  - [ ] Support for READMEs, EML and variables files
- [X] Instrumental data
  - [X] Download specific average intervals
- [ ] AOP data
- [ ] EC data
- [ ] Taxon data manip
- [X] API token
- [X] package for pypi (check the pypi branch)
