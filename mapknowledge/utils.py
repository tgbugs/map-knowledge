#
#  Flatmap viewer and annotation tools
#
#  Copyright (c) 2019-21  David Brooks
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#===============================================================================

try:
    from mapmaker.utils import log
except ImportError:
    import logging as log

#===============================================================================

from json.decoder import JSONDecodeError
import requests

LOOKUP_TIMEOUT = 5    # seconds; for `requests.get()`

#===============================================================================

def request_json(endpoint, **kwds):
    try:
        response = requests.get(endpoint,
                                headers={'accept': 'application/json'},
                                timeout=LOOKUP_TIMEOUT,
                                **kwds)
        if response.status_code == requests.codes.ok:
            try:
                return response.json()
            except json.JSONDecodeError:
                error = 'invalid JSON returned'
        else:
            error = 'status: {}'.format(response.status_code)
    except requests.exceptions.RequestException as exception:
        error = 'exception: {}'.format(exception)
    log.warning("Couldn't access {}: {}".format(endpoint, error))
    return None

#===============================================================================
