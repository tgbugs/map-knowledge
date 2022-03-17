from pprint import pprint
from mapknowledge import KnowledgeStore

MODEL_URI = 'https://apinatomy.org/uris/models/ard-arm-cardiac'

MODEL_ABBRV = 'aacar'

def NEURON_URI(n):
    return f'ilxtr:neuron-type-{MODEL_ABBRV}-{n}'

def print_knowledge(store, entity):
    print(f'{entity}:')
    pprint(store.entity_knowledge(entity))
    print()

def print_phenotypes(store, entity):
    print("Querying", entity)
    knowledge = store.entity_knowledge(entity)
    print(f'{entity}: {knowledge.get("phenotypes", [])}')

if __name__ == '__main__':
    store = KnowledgeStore(store_directory='.')

    print_knowledge(store, MODEL_URI)
    for n in [13]:
        print_knowledge(store, NEURON_URI(n))

    store.close()
