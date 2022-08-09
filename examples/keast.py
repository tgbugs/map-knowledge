from pprint import pprint
from mapknowledge import KnowledgeStore
from mapknowledge.scicrunch import SCICRUNCH_PRODUCTION, SCICRUNCH_STAGING

KEAST_MODEL = 'https://apinatomy.org/uris/models/keast-bladder'

def KEAST_NEURON(n):
    return f'ilxtr:neuron-type-keast-{n}'

def print_knowledge(store, entity):
    print(f'{entity}:')
    pprint(store.entity_knowledge(entity))
    print()

def print_phenotypes(store, entity):
    print("Querying", entity)
    knowledge = store.entity_knowledge(entity)
    print(f'{entity}: {knowledge.get("phenotypes", [])}')

if __name__ == '__main__':
    print('Production:')
    store = KnowledgeStore(scicrunch_release=SCICRUNCH_PRODUCTION)
    print_knowledge(store, KEAST_MODEL)
    print_knowledge(store, KEAST_NEURON(9))
    store.close()

    print('Staging:')
    store = KnowledgeStore(scicrunch_release=SCICRUNCH_STAGING)
    print_knowledge(store, KEAST_MODEL)
    print_knowledge(store, KEAST_NEURON(9))
    store.close()
