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

__version__ = "0.10.4"

#===============================================================================

import sqlite3
import datetime
import json
import os

from pathlib import Path

#===============================================================================

from .scicrunch import APINATOMY_MODEL_PREFIX, CONNECTIVITY_ONTOLOGIES
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
            # Create knowledge base if it doesn't exist
            self.__db_name = Path(store_directory, knowledge_base).resolve()
            if create and not self.__db_name.exists():
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
        if scicrunch_api is not None:
            self.__scicrunch = SciCrunch(api_endpoint=scicrunch_api,
                                         scicrunch_release=scicrunch_release,
                                         scicrunch_key=scicrunch_key)
        else:
            self.__scicrunch = None
        # Optionally clear local connectivity knowledge from SciCrunch
        if (self.db is not None and clean_connectivity):
            entities = [f'entity like {APINATOMY_MODEL_PREFIX}%']
            entities.extend([f'entity like {ontology}:%' for ontology in CONNECTIVITY_ONTOLOGIES])
            pattern = (' or '.join(entities),)
            log.info(f'Clearing connectivity knowledge...')
            self.db.execute('delete from knowledge where ?', pattern)
            self.db.execute('delete from labels where ?', pattern)
            self.db.execute('delete from publications where ?', pattern)

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
            return sorted(models.keys())
        elif self.db is not None:
            return [row[0] for row in self.db.execute('select model from connectivity_models order by model')]
        else:
            return []

    def labels(self):
    #================
        if self.db is not None:
            return [tuple(row) for row in self.db.execute('select entity, label from labels order by entity')]
        else:
            return []

    def entity_knowledge(self, entity):
    #==================================
        # Optionally refresh local connectivity knowledge from SciCrunch
        if self.db is not None:
            # Check local cache
            knowledge = self.__entity_knowledge.get(entity, {})
            if len(knowledge): return knowledge

        knowledge = {}
        if self.db is not None:
            # Check our database
            row = self.db.execute('select knowledge from knowledge where entity=?', (entity,)).fetchone()
            if row is not None:
                knowledge = json.loads(row[0])
        if len(knowledge) == 0 and self.__scicrunch is not None:
            # Consult SciCrunch if we don't know about the entity
            knowledge = self.__scicrunch.get_knowledge(entity)
            if 'connectivity' in knowledge:
                phenotypes = self.__scicrunch.get_phenotypes(entity)
                if len(phenotypes) > 0:
                    knowledge['phenotypes'] = phenotypes
            if len(knowledge) > 0 and self.db is not None and not self.read_only:
                if not self.db.in_transaction:
                    self.db.execute('begin')
                # Save knowledge in our database
                self.db.execute('replace into knowledge values (?, ?)', (entity, json.dumps(knowledge)))
                # Save label and references in their own tables
                if 'label' in knowledge:
                    self.db.execute('replace into labels values (?, ?)', (entity, knowledge['label']))
                if 'references' in knowledge:
                    self.update_references(entity, knowledge.get('references', []))
                self.db.commit()
        # Use the entity's value as its label if none is defined
        if 'label' not in knowledge:
            knowledge['label'] = entity
        # Cache local knowledge
        self.__entity_knowledge[entity] = knowledge
        return knowledge

    def label(self, entity):
    #=======================
        if self.db is not None:
            row = self.db.execute('select label from labels where entity=?', (entity,)).fetchone()
            if row is not None:
                return row[0]
        knowledge = self.entity_knowledge(entity)
        return knowledge['label']

    def update_references(self, entity, references):
    #===============================================
        if self.db is not None:
            with self.db:
                self.db.execute('delete from publications where entity = ?', (entity, ))
                self.db.executemany('insert into publications(entity, publication) values (?, ?)',
                    ((entity, reference) for reference in references))

#===============================================================================

