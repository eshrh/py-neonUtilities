import glob
from importlib import reload
import neon
reload(neon)
import os
import zipfile
import re

class NeonInstrumental(neon.Neon):
    def __init__(self):
