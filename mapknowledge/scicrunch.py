#===============================================================================
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

import os

#===============================================================================

from .apinatomy import Apinatomy
from .utils import log
from .utils import request_json

#===============================================================================

INTERLEX_ONTOLOGIES = ['ILX', 'NLX']

CONNECTIVITY_ONTOLOGIES = [ 'ilxtr' ]

APINATOMY_MODEL_PREFIX = 'https://apinatomy.org/uris/models/'

#===============================================================================

SCICRUNCH_API_ENDPOINT = 'https://scicrunch.org/api/1'

#===============================================================================

# Values for SCICRUNCH_RELEASE
SCICRUNCH_PRODUCTION = 'sckan-scigraph'
SCICRUNCH_STAGING = 'sparc-scigraph'

#===============================================================================

SCICRUNCH_INTERLEX_VOCAB = '{API_ENDPOINT}/ilx/search/curie/{TERM}'
SCICRUNCH_SPARC_API = '{API_ENDPOINT}/{SCICRUNCH_RELEASE}'

SCICRUNCH_SPARC_CYPHER = f'{SCICRUNCH_SPARC_API}/cypher/execute.json'
SCICRUNCH_SPARC_VOCAB = f'{SCICRUNCH_SPARC_API}/vocabulary/id/{{TERM}}.json'

SCICRUNCH_SPARC_APINATOMY = f'{SCICRUNCH_SPARC_API}/dynamic/demos/apinat'
SCICRUNCH_CONNECTIVITY_MODELS = f'{SCICRUNCH_SPARC_APINATOMY}/modelList.json'
SCICRUNCH_CONNECTIVITY_NEURONS = f'{SCICRUNCH_SPARC_APINATOMY}/neru-5/{{NEURON_ID}}.json'

#===============================================================================

class NAMESPACES:
    namespaces = {
        'ilxtr': 'http://uri.interlex.org/tgbugs/uris/readable/'
    }

    @staticmethod
    def uri(curie: str) -> str:
        parts = curie.split(':', 1)
        if len(parts) == 2 and parts[0] in NAMESPACES.namespaces:
            return NAMESPACES.namespaces[parts[0]] + parts[1]
        return curie

    @staticmethod
    def curie(uri: str) -> str:
        for prefix, ns_uri in NAMESPACES.namespaces.items():
            if uri.startswith(ns_uri):
                return f'{prefix}:{uri[len(ns_uri):]}'
        return uri

#===============================================================================

class SciCrunch(object):
    def __init__(self, api_endpoint=SCICRUNCH_API_ENDPOINT, scicrunch_release=SCICRUNCH_STAGING, scicrunch_key=None):
        self.__api_endpoint = api_endpoint
        self.__scicrunch_release = scicrunch_release
        self.__sparc_api_endpoint = SCICRUNCH_SPARC_API.format(API_ENDPOINT=api_endpoint,
                                                               SCICRUNCH_RELEASE=scicrunch_release)
        self.__unknown_entities = []
        self.__scicrunch_key = scicrunch_key if scicrunch_key is not None else os.environ.get('SCICRUNCH_API_KEY')
        if self.__scicrunch_key is None:
            log.warning('Undefined SCICRUNCH_API_KEY: SciCrunch knowledge will not be looked up')

    @property
    def sparc_api_endpoint(self):
        return self.__sparc_api_endpoint
    def get_knowledge(self, entity: str) -> dict:
    #============================================
        knowledge = {}
        if self.__scicrunch_key is not None:
            params = {
                'api_key': self.__scicrunch_key,
                'limit': 9999,
            }
            ontology = entity.split(':')[0]
            if   ontology in INTERLEX_ONTOLOGIES:
                data = request_json(SCICRUNCH_INTERLEX_VOCAB.format(API_ENDPOINT=self.__api_endpoint,
                                                                    SCICRUNCH_RELEASE=self.__scicrunch_release,
                                                                    TERM=entity),
                                    params=params)
                if data is not None:
                    knowledge['label'] = data.get('data', {}).get('label', entity)
            elif ontology in CONNECTIVITY_ONTOLOGIES:
                data = request_json(SCICRUNCH_CONNECTIVITY_NEURONS.format(API_ENDPOINT=self.__api_endpoint,
                                                                          SCICRUNCH_RELEASE=self.__scicrunch_release,
                                                                          NEURON_ID=entity),
                                    params=params)
                if data is not None:
                    knowledge = Apinatomy.neuron_knowledge(entity, data)
            elif entity.startswith(APINATOMY_MODEL_PREFIX):
                params['cypherQuery'] = Apinatomy.neurons_for_model_cypher(entity)
                data = request_json(SCICRUNCH_SPARC_CYPHER.format(API_ENDPOINT=self.__api_endpoint,
                                                                  SCICRUNCH_RELEASE=self.__scicrunch_release),
                                    params=params)
                if data is not None:
                    knowledge = Apinatomy.model_knowledge(entity, data)
            else:
                data = request_json(SCICRUNCH_SPARC_VOCAB.format(API_ENDPOINT=self.__api_endpoint,
                                                                 SCICRUNCH_RELEASE=self.__scicrunch_release,
                                                                 TERM=entity),
                                    params=params)
                if data is not None:
                    if len(labels := data.get('labels', [])):
                        knowledge['label'] = labels[0]
                    else:
                        knowledge['label'] = entity
        if len(knowledge) == 0 and entity not in self.__unknown_entities:
            log.warning('Unknown anatomical entity: {}'.format(entity))
            self.__unknown_entities.append(entity)
        return knowledge

    def get_phenotypes(self, entity: str) -> list:
    #=============================================
        phenotypes = None
        if self.__scicrunch_key is not None:
            params = {
                'api_key': self.__scicrunch_key,
                'limit': 9999,
            }
            params['cypherQuery'] = Apinatomy.phenotype_for_neuron_cypher(NAMESPACES.uri(entity))
            data = request_json(SCICRUNCH_SPARC_CYPHER.format(API_ENDPOINT=self.__api_endpoint,
                                                              SCICRUNCH_RELEASE=self.__scicrunch_release),
                                params=params)
            if data is not None:
                phenotypes = Apinatomy.phenotypes(data)
        if phenotypes is None and entity not in self.__unknown_entities:
            log.warning('Unknown anatomical entity: {}'.format(entity))
            self.__unknown_entities.append(entity)
        return phenotypes

#===============================================================================
