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

__version__ = "0.9.0"

#===============================================================================

import sqlite3
import datetime
import json

from pathlib import Path

#===============================================================================

from .scicrunch import APINATOMY_MODEL_PREFIX, CONNECTIVITY_ONTOLOGIES, SCICRUNCH_API_ENDPOINT
from .scicrunch import SciCrunch
from .utils import log

#===============================================================================

KNOWLEDGE_BASE = 'knowledgebase.db'

#===============================================================================

KNOWLEDGE_SCHEMA = """
    begin;
    create table knowledge (entity text primary key, knowledge text);
    create unique index knowledge_index on knowledge(entity);

    create table labels (entity text primary key, label text);
    create unique index labels_index on labels(entity);

    create table publications (entity text, publication text);
    create index publications_entity_index on publications(entity);
    create index publications_publication_index on publications(publication);
    commit;
"""

#===============================================================================

class KnowledgeBase(object):
    def __init__(self, store_directory, read_only=False, create=False, knowledge_base=KNOWLEDGE_BASE):
        if store_directory is None:
            self.__db_name = None
            self.__db = None
        else:
            self.__db_name = Path(store_directory, knowledge_base).resolve()
            if create and not self.__db_name.exists():
                db = sqlite3.connect(self.__db_name,
                    detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
                db.executescript(KNOWLEDGE_SCHEMA)
                db.close()
            ## What about upgrading when tables (e.g. knowledge) don't exist???
            db_uri = '{}?mode=ro'.format(self.__db_name.as_uri()) if read_only else self.__db_name.as_uri()
            self.__db = sqlite3.connect(db_uri, uri=True,
                detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

    @property
    def db(self):
        return self.__db

    @property
    def db_name(self):
        return self.__db_name

    def close(self):
        if self.__db is not None:
            self.__db.close()

#===============================================================================

class KnowledgeStore(KnowledgeBase):
    def __init__(self, store_directory=None,
                       knowledge_base=KNOWLEDGE_BASE,
                       clean_connectivity=False,
                       scicrunch_api=SCICRUNCH_API_ENDPOINT):
        super().__init__(store_directory, create=True, knowledge_base=knowledge_base)
        self.__knowledge_base = (store_directory is not None)
        self.__clean_connectivity = clean_connectivity
        self.__entity_knowledge = {}     # Cache lookups
        self.__scicrunch = SciCrunch(scicrunch_api)
        self.__refreshed = []

    def entity_knowledge(self, entity):
    #==================================
        # Optionally refresh local connectivity knowledge from SciCrunch
        if (self.__knowledge_base
         and self.__clean_connectivity
         and (entity.startswith(APINATOMY_MODEL_PREFIX)
           or entity.split(':')[0] in CONNECTIVITY_ONTOLOGIES)
         and entity not in self.__refreshed):
            log.info(f'Refreshing knowledge for {entity}')
            self.db.execute('delete from knowledge where entity=?', (entity,))
            self.__refreshed.append(entity)
        else:
            # Check local cache
            knowledge = self.__entity_knowledge.get(entity, {})
            if len(knowledge): return knowledge

        knowledge = {}
        if self.__knowledge_base:
            # Check our database
            row = self.db.execute('select knowledge from knowledge where entity=?', (entity,)).fetchone()
            if row is not None:
                knowledge = json.loads(row[0])
        if len(knowledge) == 0:
            # Consult SciCrunch if we don't know about the entity
            knowledge = self.__scicrunch.get_knowledge(entity)
            if len(knowledge) > 0 and self.__knowledge_base:
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
        row = self.db.execute('select label from labels where entity=?', (entity,)).fetchone()
        if row is not None:
            return row[0]
        knowledge = self.entity_knowledge(entity)
        return knowledge['label']

    def update_references(self, entity, references):
    #===============================================
        with self.db:
            self.db.execute('delete from publications where entity = ?', (entity, ))
            self.db.executemany('insert into publications(entity, publication) values (?, ?)',
                ((entity, reference) for reference in references))

#===============================================================================

