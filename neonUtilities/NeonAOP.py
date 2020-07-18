# This file is part of py-neonUtilities.

# py-neonUtilities is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# py-neonUtilities is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with py-neonUtilities.  If not, see <https://www.gnu.org/licenses/>.

from importlib import reload
import neon
reload(neon)

class NeonAOP(neon.Neon):
    def __init__(self, dpID=None, site=None, year=None):
        #TODO error checking.
        pass
