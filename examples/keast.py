from pprint import pprint
from mapknowledge import KnowledgeStore

KEAST_MODEL = 'https://apinatomy.org/uris/models/keast-bladder'
KEAST_NEURON_1 = 'ilxtr:neuron-type-keast-1'
KEAST_NEURON_12 = 'ilxtr:neuron-type-keast-12'

def print_knowledge(store, entity):
    print(f'Knowledge about {entity}:')
    pprint(store.entity_knowledge(entity))
    print()

if __name__ == '__main__':
    store = KnowledgeStore('.')
    print_knowledge(store, KEAST_MODEL)
    print_knowledge(store, KEAST_NEURON_1)
    print_knowledge(store, KEAST_NEURON_12)
    store.close()
