#===============================================================================
#
#  Flatmap viewer and annotation tools
#
#  Copyright (c) 2019-22  David Brooks
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

__version__ = "0.13.2"

#===============================================================================

import sqlite3
import json
import os

from pathlib import Path

#===============================================================================

from .apinatomy import CONNECTIVITY_ONTOLOGIES, APINATOMY_MODEL_PREFIX
from .scicrunch import SCICRUNCH_API_ENDPOINT, SCICRUNCH_PRODUCTION, SCICRUNCH_STAGING
from .scicrunch import SciCrunch
from .utils import log

#===============================================================================

KNOWLEDGE_BASE = 'knowledgebase.sqlite'

#===============================================================================

KNOWLEDGE_SCHEMA = """
    begin;
    create table metadata (name text primary key, value text);

    create table knowledge (entity text primary key, knowledge text);
    create unique index knowledge_index on knowledge(entity);

    create table labels (entity text primary key, label text);
    create unique index labels_index on labels(entity);

    create table publications (entity text, publication text);
    create index publications_entity_index on publications(entity);
    create index publications_publication_index on publications(publication);

    create table connectivity_models (model text primary key);
    commit;
"""

#===============================================================================

class KnowledgeBase(object):
    def __init__(self, store_directory, read_only=False, create=False, knowledge_base=KNOWLEDGE_BASE):
        self.__db = None
        self.__read_only = read_only
        if store_directory is None:
            self.__db_name = None
        else:
            # Create store directory if it doesn't exist
            if not os.path.exists(store_directory):
                os.makedirs(store_directory)
            # Create knowledge base if it doesn't exist and we are allowed to
            self.__db_name = Path(store_directory, knowledge_base).resolve()
            if not self.__db_name.exists():
                if not create:
                    raise IOError(f'Missing KnowledgeBase: {self.__db_name}')
                db = sqlite3.connect(self.__db_name,
                    detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
                db.executescript(KNOWLEDGE_SCHEMA)
                db.close()
            ## What about upgrading when tables (e.g. knowledge) don't exist???
            self.open(read_only=read_only)

    @property
    def db(self):
        return self.__db

    @property
    def db_name(self):
        return self.__db_name

    @property
    def read_only(self):
        return self.__read_only

    def close(self):
        if self.__db is not None:
            self.__db.close()
            self.__db = None

    def open(self, read_only=False):
        self.close()
        db_uri = '{}?mode=ro'.format(self.__db_name.as_uri()) if read_only else self.__db_name.as_uri()
        self.__db = sqlite3.connect(db_uri, uri=True,
                                    detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

    def metadata(self, name):
        row = self.__db.execute('select value from metadata where name=?', (name,)).fetchone()
        if row is not None:
            return row[0]

    def set_metadata(self, name, value):
        if not self.__db.in_transaction:
            self.__db.execute('begin')
        self.db.execute('replace into metadata values (?, ?)', (name,value))
        self.__db.execute('commit')

#===============================================================================

class KnowledgeStore(KnowledgeBase):
    def __init__(self, store_directory=None,
                       knowledge_base=KNOWLEDGE_BASE,
                       clean_connectivity=False,
                       scicrunch_api=SCICRUNCH_API_ENDPOINT,
                       scicrunch_release=SCICRUNCH_PRODUCTION,
                       scicrunch_key=None,
                       create=True,
                       read_only=False):
        super().__init__(store_directory, create=create, knowledge_base=knowledge_base, read_only=read_only)
        self.__entity_knowledge = {}     # Cache lookups

        if (db_name := self.db_name) is not None:
            cache_msg = f'with cache {db_name}'
        else:
            cache_msg = f'with no cache'
        if scicrunch_api is not None:
            self.__scicrunch = SciCrunch(api_endpoint=scicrunch_api,
                                         scicrunch_release=scicrunch_release,
                                         scicrunch_key=scicrunch_key)
            built = f" built at {build['released']}" if (build := self.__scicrunch.sckan_build()) is not None else ''
            release = 'production' if scicrunch_release == SCICRUNCH_PRODUCTION else 'staging'
            scicrunch_msg = f"using {release} SCKAN{built} from {self.__scicrunch.sparc_api_endpoint}"
        else:
            self.__scicrunch = None
            scicrunch_msg = 'not using SCKAN'
        log.info(f'Map Knowledge version {__version__} {cache_msg} {scicrunch_msg}')
        # Optionally clear local connectivity knowledge from SciCrunch
        if (self.db is not None and clean_connectivity):
            log.info(f'Clearing connectivity knowledge...')
            entities = [f'{APINATOMY_MODEL_PREFIX}%']
            entities.extend([f'{ontology}:%' for ontology in CONNECTIVITY_ONTOLOGIES])
            condition = ' or '.join(len(entities)*['entity like ?'])
            self.db.execute('begin')
            self.db.execute(f'delete from knowledge where {condition}', tuple(entities))
            self.db.execute(f'delete from labels where {condition}', tuple(entities))
            self.db.execute(f'delete from publications where {condition}', tuple(entities))
            self.db.execute('commit')

    @property
    def scicrunch(self):
        return self.__scicrunch

    def connectivity_models(self):
    #=============================
        if self.__scicrunch is not None:
            models = self.__scicrunch.connectivity_models()
            if self.db is not None and not self.read_only:
                if not self.db.in_transaction:
                    self.db.execute('begin')
                for model, label in models.items():
                    self.db.execute('replace into connectivity_models values (?)', (model, ))
                    self.db.execute('replace into labels values (?, ?)', (model, label))
                self.db.commit()
            return models
        elif self.db is not None:
            return {row[0]: row[1] for row in self.db.execute('''
                select c.model, l.label from connectivity_models as c
                    left join labels as l on c.model = l.entity order by model
                ''')}
        else:
            return {}

    def labels(self):
    #================
        if self.db is not None:
            return [tuple(row) for row in self.db.execute('select entity, label from labels order by entity')]
        else:
            return []

    @staticmethod
    def __log_errors(entity, knowledge):
    #===================================
        for error in knowledge.get('errors', []):
            log.error(f'SCKAN knowledge error: {entity}: {error}')

    def entity_knowledge(self, entity):
    #==================================
        # Optionally refresh local connectivity knowledge from SciCrunch
        if self.db is not None:
            # Check local cache
            knowledge = self.__entity_knowledge.get(entity, {})
            if len(knowledge):
                KnowledgeStore.__log_errors(entity, knowledge)
                return knowledge

        knowledge = {}
        if self.db is not None:
            # Check our database
            row = self.db.execute('select knowledge from knowledge where entity=?', (entity,)).fetchone()
            if row is not None:
                knowledge = json.loads(row[0])
        if (self.__scicrunch is not None
         and (len(knowledge) == 0 or entity == knowledge.get('label', entity))):
            # Consult SciCrunch if we don't know about the entity
            knowledge = self.__scicrunch.get_knowledge(entity)
            if 'connectivity' in knowledge:
                # Get phenotype, taxon, and other metadate
                knowledge.update(self.__scicrunch.connectivity_metadata(entity))
                # Make sure we have labels for each entity used for connectivity
                connectivity_terms = set()
                for (node0, node1) in knowledge['connectivity']:
                    connectivity_terms.update([node0[0], node1[0]])
                    connectivity_terms.update(node0[1])
                    connectivity_terms.update(node1[1])
                for connectivity_term in connectivity_terms:
                    self.label(connectivity_term)
            if len(knowledge) > 0 and self.db is not None and not self.read_only:
                if not self.db.in_transaction:
                    self.db.execute('begin')
                # Use 'long-label' if the entity's label' is the same as itself.
                if 'label' in knowledge:
                    if knowledge['label'] == entity and 'long-label' in knowledge:
                        knowledge['label'] = knowledge['long-label']                # Save knowledge in our database
                self.db.execute('replace into knowledge values (?, ?)', (entity, json.dumps(knowledge)))
                # Save label and references in their own tables
                if 'label' in knowledge:
                    self.db.execute('replace into labels values (?, ?)', (entity, knowledge['label']))
                if 'references' in knowledge:
                    self.__update_references(entity, knowledge.get('references', []))
                self.db.commit()

        # Use the entity's value as its label if none is defined
        if 'label' not in knowledge:
            knowledge['label'] = entity

        # Cache local knowledge
        self.__entity_knowledge[entity] = knowledge

        # Log any errors
        KnowledgeStore.__log_errors(entity, knowledge)

        return knowledge

    def label(self, entity):
    #=======================
        if self.db is not None:
            row = self.db.execute('select label from labels where entity=?', (entity,)).fetchone()
            if row is not None:
                return row[0]
        knowledge = self.entity_knowledge(entity)
        return knowledge['label']

    def __update_references(self, entity, references):
    #===============================================
        if self.db is not None:
            with self.db:
                self.db.execute('delete from publications where entity = ?', (entity, ))
                self.db.executemany('insert into publications(entity, publication) values (?, ?)',
                    ((entity, reference) for reference in references))

#===============================================================================

