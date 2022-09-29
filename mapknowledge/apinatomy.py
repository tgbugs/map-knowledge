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
#
# Acknowledgements:
#   This code is based on ``connectivity pairs`` from
#   `here <https://github.com/SciCrunch/sparc-curation/blob/master/docs/queries.org#connectivity-based>`_
#   and ``pyontutils.core`` and ``nifstd_tools.simplify`` in
#   https://github.com/tgbugs/pyontutils and has been reworked to remove the need
#   to install the full ``pyontutils`` package along with its dependencies.
#
#===============================================================================

import networkx as nx
import rdflib
from rdflib.extras import external_graph_libs as egl

#===============================================================================

from .utils import log

# Layers shouldn't be resolving to
# ``spinal cord``, etc. nor to ``None``.
# A SCKAN issue
EXCLUDED_LAYERS = [
    None,
    'UBERON:0000010',      # peripheral nervous system
    'UBERON:0000178',      # blood
    'UBERON:0000468',      # multicellular organism
    'UBERON:0001017',      # central nervous system
    'UBERON:0001359',      # cerebrospinal fluid
    'UBERON:0002318',      # spinal cord white matter
    'UBERON:0003714',      # neural tissue
    'UBERON:0005844',      # spinal cord segment
    'UBERON:0016549',      # cns white matter
]

#===============================================================================

class PyOntUtilsEdge(tuple):
    """ Expansion of curies must happen before construction if it is going to
        happen at all. The expansion rule must be known beforehand. """

    @classmethod
    def fromNx(cls, edge):
        s, o, p = [e.toPython() if isinstance(e, rdflib.URIRef) else e
                   for e in edge]  # FIXME need to curie here or elsewhere?
        t = (s, p, o)
        self = cls(t)
        return self

    @classmethod
    def fromOboGraph(cls, blob):
        t = blob['sub'], blob['pred'], blob['obj']
        self = cls(t)
        self._blob = blob
        return self

    @property
    def s(self): return self[0]

    @property
    def p(self): return self[1]

    @property
    def o(self): return self[2]

    subject = s
    predicate = p
    object = o

    def asTuple(self):
        return (*self,)

    def asRdf(self):
        """ Note that no expansion may be done at this time. """
        return tuple(e if isinstance(e, rdflib.URIRef) else rdflib.URIRef(e) for e in self)

    def asOboGraph(self):
        if not hasattr(self, '_blob'):
            self._blob = {k:e for k, e in zip(('sub', 'pred', 'obj'), self)}
        return self._blob

#===============================================================================

class nifstd:
    @staticmethod
    def sub(edge, match=None):
        return edge['sub'] == match if match is not None else edge['sub']

    @staticmethod
    def pred(edge, match=None):
        return edge['pred'] == match if match is not None else edge['pred']

    @staticmethod
    def obj(edge, match=None):
        return edge['obj'] == match if match is not None else edge['obj']

    @staticmethod
    def ematch(blob, select, match, *matches, matchf=lambda ms: True):
        return [edge for edge in blob['edges'] if select(edge, match) and matchf(matches)]

    @staticmethod
    def listIn(container, maybe_contained, *, strict=True):
        """ strictly sublists no equality here """
        lc = len(container)
        lmc = len(maybe_contained)
        if lc > lmc or not strict and lc == lmc:
            z = maybe_contained[0]
            if z in container:
                substart = container.index(z)
                subcontained = maybe_contained[1:]
                if not subcontained:
                    return substart
                ssp1 = substart + 1
                subcontainer = container[ssp1:]
                maybe = nifstd.listIn(subcontainer, subcontained, strict=False)
                if maybe is None or maybe > 0:
                    maybe = nifstd.listIn(subcontainer, maybe_contained, strict=False)
                    if maybe is not None:
                        return ssp1 + maybe
                else:
                    return substart

    @staticmethod
    def zap(ordered_nodes, predicates, oe2, blob):
        """ don't actually zap, wait until the end so that all
            deletions happen after all additions """
        e = PyOntUtilsEdge((ordered_nodes[0].toPython(),
                '-'.join(predicates),
                ordered_nodes[-1].toPython()))
        new_e = e.asOboGraph()
        blob['edges'].append(new_e)
        to_remove = [e.asOboGraph() for e in oe2]
        return to_remove

    @staticmethod
    def simplify(collapse, blob):
        to_remove = []
        for coll in collapse:
            exclude = set(p for p in coll)
            candidates = [e for e in blob['edges'] if e['pred'] in exclude]
            for c in candidates:
                # make sure we can remove the edges later
                # if they have meta the match will fail
                if 'meta' in c:
                    c.pop('meta')
            if candidates:
                edges = [PyOntUtilsEdge.fromOboGraph(c) for c in candidates]
                g = rdflib.Graph()
                for e in edges: g.add(e.asRdf())
                nxg = egl.rdflib_to_networkx_multidigraph(g)
                connected = list(nx.weakly_connected_components(nxg))  # FIXME may not be minimal
                ends = [e.asRdf()[-1] for e in edges if e.p == coll[-1]]
                for c in connected:
                    #log.debug('\n' + pformat(c))
                    nxgt = nx.MultiDiGraph()
                    nxgt.add_edges_from(nxg.edges(c, keys=True))
                    ordered_nodes = list(nx.topological_sort(nxgt))
                    paths = [p
                             for n in nxgt.nodes()
                             for e in ends
                             for p in list(nx.all_simple_paths(nxgt, n, e))
                             if len(p) == len(coll) + 1]
                    for path in sorted(paths):
                        ordered_edges = nxgt.edges(path, keys=True)
                        oe2 = [PyOntUtilsEdge.fromNx(e) for e in ordered_edges if all([n in path for n in e[:2]])]
                        predicates = [e.p for e in oe2]
                        #log.debug('\n' + pformat(oe2))
                        if predicates == coll: #in collapse:
                            to_remove.extend(nifstd.zap(path, predicates, oe2, blob))
                        else:  # have to retain this branch to handle cases where the end predicate is duplicated
                            #log.error('\n' + pformat(predicates) +
                            #            '\n' + pformat(coll))
                            for preds in [coll]:
                                sublist_start = nifstd.listIn(predicates, preds)
                                if sublist_start is not None:
                                    i = sublist_start
                                    j = i + len(preds)
                                    npath = path[i:j + 1]  # + 1 to include final node
                                    oe2 = oe2[i:j]
                                    predicates = predicates[i:j]
                                    to_remove.extend(nifstd.zap(npath, predicates, oe2, blob))
        for r in to_remove:
            if r in blob['edges']:
                blob['edges'].remove(r)
        #log.debug('\n' + pformat(blob['edges']))
        return blob  # note that this is in place modification so sort of supruflous

#===============================================================================

class Apinatomy:
    axon = 'SAO:280355188'
    dendrite = 'SAO:420754792'
    BAG = 'apinatomy:BAG'
    annotates = 'apinatomy:annotates'
    cloneOf = 'apinatomy:cloneOf'
    endsIn = 'apinatomy:endsIn'
    ontologyTerms = 'apinatomy:ontologyTerms'
    fasciculatesIn = 'apinatomy:fasciculatesIn'
    inheritedExternal = 'apinatomy:inheritedExternal'
    inheritedExternal_s = 'apinatomy:inheritedExternal*'
    internalIn = 'apinatomy:internalIn'
    layerIn = 'apinatomy:layerIn'
    lyphs = 'apinatomy:lyphs'
    next = 'apinatomy:next'
    next_s = 'apinatomy:next*'
    references = 'apinatomy:references'
    topology_s = 'apinatomy:topology*'

    @staticmethod
    def neurons_for_model_cypher(model_id):
        # From https://github.com/SciCrunch/sparc-curation/blob/master/docs/queries.org#neru-model-populations
        # See also https://github.com/SciCrunch/sparc-curation/blob/master/docs/queries.org#neru-model-populations-and-references
        return """
            MATCH (start:Ontology {{iri: "{MODEL_ID}"}})
            <-[:isDefinedBy]-(external:Class)
            -[:subClassOf*]->(:Class {{iri: "http://uri.interlex.org/tgbugs/uris/readable/NeuronEBM"}}) // FIXME
            RETURN external
        """.format(MODEL_ID=model_id)

    @staticmethod
    def phenotype_for_neuron_cypher(neuron_id):
        # From https://github.com/SciCrunch/sparc-curation/blob/master/docs/queries.org#phenotypes-for-neuron
        return """
            MATCH (neupop:Class{{iri: "{NEURON_ID}"}})-[e:ilxtr:hasPhenotype!]->(phenotype) RETURN e
        """.format(NEURON_ID=neuron_id)

    @staticmethod
    def deblob(blob, remove_converge=False):
        # FIXME I think we may be over or under simplifying just a bit
        # somehow getting double links at the end of the chain

        # FIXME issue here is that chain roots -> levels goes to all levels of the chain which is NOT
        # what we want, TODO need to filter out cases where the target of levels is pointed to by next
        # this is implemented downstream from here I think
        blob['edges'] = [
            e for e in blob['edges'] if not nifstd.pred(e, 'apinatomy:levels') or
            (nifstd.pred(e, 'apinatomy:levels') and
             not nifstd.ematch(
                blob,
                lambda ei, m: (nifstd.obj(ei, m) and nifstd.pred(ei, 'apinatomy:next')),
                nifstd.obj(e))
        )]

        #[e for e in blob['edges'] if pred(e, 'apinatomy:rootOf')]

        blob = nifstd.simplify(
            [['apinatomy:target', 'apinatomy:rootOf', 'apinatomy:levels'],
             ['apinatomy:conveyingLyph', 'apinatomy:topology'],
             ['apinatomy:conveys', 'apinatomy:source', 'apinatomy:sourceOf'],
             ['apinatomy:conveys', 'apinatomy:target', 'apinatomy:sourceOf'],
             ['apinatomy:cloneOf', 'apinatomy:inheritedExternal'],
             ['apinatomy:conveyingLyph', 'apinatomy:inheritedExternal'],],
            blob)
        edges = blob['edges']
        nindex = {n['id']:n for n in blob['nodes']}  # FIXME silent errors ;_;
        for e in edges:
            if e['pred'] == 'apinatomy:nextChainStartLevels':
                e['pred'] = 'apinatomy:next'
            if e['pred'] in (
                    'apinatomy:target-apinatomy:rootOf-apinatomy:levels',
                    'apinatomy:conveys-apinatomy:source-apinatomy:sourceOf',
                    'apinatomy:conveys-apinatomy:target-apinatomy:sourceOf',):
                e['pred'] = 'apinatomy:next*'
            if e['pred'] == 'apinatomy:conveyingLyph-apinatomy:topology':
                e['pred'] = 'apinatomy:topology*'
            if e['pred'] in (
                    'apinatomy:conveyingLyph-apinatomy:inheritedExternal',
                    'apinatomy:cloneOf-apinatomy:inheritedExternal',):
                e['pred'] = 'apinatomy:inheritedExternal*'
            if nifstd.pred(e, Apinatomy.topology_s):
                # move topology to be a property not a node to make the layout cleaner
                nindex[nifstd.sub(e)]['topology'] = nifstd.obj(e)

        if remove_converge:
            # remove topology edges
            edges = blob['edges'] = [e for e in edges if not nifstd.pred(e, top)]
            # remove process type edges
            edges = blob['edges'] = [e for e in edges if not (nifstd.obj(e, Apinatomy.axon)
                                                           or nifstd.obj(e, Apinatomy.dendrite))]

        blob['edges'] = [dict(s) for s in set(frozenset({k:v for k, v in d.items()
                                                         if k != 'meta'}.items()) for d in blob['edges'])]

        def sekey(e):
            s, p, o = nifstd.sub(e), nifstd.pred(e), nifstd.obj(e)
            iot = p != Apinatomy.ontologyTerms
            return iot, p, s, o

        blob['edges'] = sorted(blob['edges'], key=sekey)
        sos = set(sov for e in blob['edges'] for sov in (e['sub'], e['obj']))
        blob['nodes'] = [n for n in blob['nodes'] if n['id'] in sos]
        somas = [e for e in edges if e['pred'] == Apinatomy.internalIn]
        terms = [e for e in edges if e['pred'] == Apinatomy.ontologyTerms]
        ordering_edges = [e for e in edges if e['pred'] == Apinatomy.next]
        return blob, edges, somas, terms, ordering_edges

    @staticmethod
    def isLayer(blob, match):
        return nifstd.ematch(
            blob,
            lambda e, m: (nifstd.sub(e, m) and nifstd.pred(e, Apinatomy.layerIn)),
            match)

    @staticmethod
    def reclr(blob, start_link):
        # recurse up the hierarchy until fasIn endIn intIn terminates
        collect = []
        layer = []
        col = True

        bads = ('UBERON:0000010', 'UBERON:0001017')  # XXX temp fix
        def select_ext(e, m, collect=collect):
            nonlocal col
            nonlocal layer
            if nifstd.sub(e, m):
                if nifstd.pred(e, Apinatomy.cloneOf):  # should be zapped during simplify
                    log.warning(f'should not have hit a cloneOf case {e}')
                    return nifstd.ematch(blob, select_ext, nifstd.obj(e))
                if (nifstd.pred(e, Apinatomy.ontologyTerms)
                 or nifstd.pred(e, Apinatomy.inheritedExternal)
                 or nifstd.pred(e, Apinatomy.inheritedExternal_s)):
                    external = nifstd.obj(e)
                    if col:
                        if layer:
                            if len(layer) > 1:  # ensure ontologyTerms get priority
                                l, *layer = layer
                                while l in bads:
                                    l, *layer = layer
                            else:
                                l = layer.pop()
                        else:
                            l = None
                        r = [b for b in blob['nodes'] if b['id'] == external][0]['id']  # if this is empty we are in big trouble
                        collect.append((l, r))
                    else:
                        l = [b for b in blob['nodes'] if b['id'] == external][0]['id']
                        layer.append(l)
                    return external

        def select(e, m):
            nonlocal col
            if nifstd.sub(e, m):
                if (nifstd.pred(e, Apinatomy.layerIn)
                 or nifstd.pred(e, Apinatomy.fasciculatesIn)
                 or nifstd.pred(e, Apinatomy.endsIn)
                 or nifstd.pred(e, Apinatomy.internalIn)):
                    col = not Apinatomy.isLayer(blob, nifstd.obj(e))
                    nifstd.ematch(blob, select_ext, nifstd.obj(e))
                    nifstd.ematch(blob, select, nifstd.obj(e))

        nifstd.ematch(blob, select, start_link)
        return collect

    @staticmethod
    def layer_regions(blob, start):
        direct = [nifstd.obj(t) for t in
                  nifstd.ematch(blob, (lambda e, m: nifstd.sub(e, m)
                                    and (nifstd.pred(e, Apinatomy.internalIn)
                                      or nifstd.pred(e, Apinatomy.endsIn)
                                      or nifstd.pred(e, Apinatomy.fasciculatesIn))),
                             start)]
        layers = [nifstd.obj(t) for d in direct for t in
                  nifstd.ematch(blob, (lambda e, m: nifstd.sub(e, m)
                                    and Apinatomy.isLayer(blob, m)
                                    and (nifstd.pred(e, Apinatomy.inheritedExternal)
                                      or nifstd.pred(e, Apinatomy.inheritedExternal_s)
                                      or nifstd.pred(e, Apinatomy.ontologyTerms))),
                             d)]
        layers = [l for l in layers if l not in ('UBERON:0000010', 'UBERON:0001017')]  # XXX temp fix
        lregs = []
        if layers:
            ldir = [nifstd.obj(t) for d in direct for t in
                    nifstd.ematch(blob, (lambda e, m: nifstd.sub(e, m)
                                      and nifstd.pred(e, Apinatomy.layerIn)),
                               d)]
            lregs = [nifstd.obj(t) for d in ldir for t in
                     nifstd.ematch(blob, (lambda e, m: nifstd.sub(e, m)
                                       and not Apinatomy.isLayer(blob, m)
                                       and (nifstd.pred(e, Apinatomy.inheritedExternal)
                                         or nifstd.pred(e, Apinatomy.inheritedExternal_s)
                                         or nifstd.pred(e, Apinatomy.ontologyTerms))),
                                d)]
        regions = [nifstd.obj(t) for d in direct for t in
                   nifstd.ematch(blob, (lambda e, m: nifstd.sub(e, m)
                                     and not Apinatomy.isLayer(blob, m)
                                     and (nifstd.pred(e, Apinatomy.inheritedExternal)
                                       or nifstd.pred(e, Apinatomy.inheritedExternal_s)
                                       or nifstd.pred(e, Apinatomy.ontologyTerms))),
                                 d)]

        lrs = Apinatomy.reclr(blob, start)

        assert not (lregs and regions), (lregs, regions)  # not both
        regions = lregs if lregs else regions
        return start, tuple(lrs)

    @staticmethod
    def find_terminals(blob, type):
        return [es for es in blob['edges']
                if nifstd.pred(es, Apinatomy.inheritedExternal_s)
                and nifstd.obj(es, type)
                and nifstd.ematch(blob, (lambda e, m: nifstd.sub(e, m)
                                         and nifstd.pred(e, Apinatomy.topology_s)
                                         and nifstd.obj(e, Apinatomy.BAG)),
                                  nifstd.sub(es))]

    @staticmethod
    def find_region(blob, edge):
        collect = []
        def select(e, m, collect=collect):
            if nifstd.sub(e, m):
                if (nifstd.pred(e, Apinatomy.layerIn)
                 or nifstd.pred(e, Apinatomy.fasciculatesIn)
                 or nifstd.pred(e, Apinatomy.endsIn)):
                    return nifstd.ematch(blob, select, nifstd.obj(e))
                elif nifstd.pred(e, Apinatomy.ontologyTerms):
                    region = nifstd.obj(e)
                    collect.extend([b for b in blob['nodes'] if b['id'] == region])
                    return region
        nifstd.ematch(blob, select, nifstd.sub(edge))
        return collect

    @staticmethod
    def find_region_layer(blob, edge, bindex):  # XXX did I just reimplement a worse reclr ???
        collect = []
        layers = []
        layers_ies = []
        donel = set()
        doner = set()
        def select_term(e, m, layers=layers):
            if nifstd.sub(e, m):
                if (nifstd.pred(e, Apinatomy.ontologyTerms)
                    or nifstd.pred(e, Apinatomy.inheritedExternal)
                    or nifstd.pred(e, Apinatomy.inheritedExternal_s)):
                    layer = nifstd.obj(e)
                    if layer not in donel:
                        donel.add(layer)
                        if (nifstd.pred(e, Apinatomy.inheritedExternal)
                            or nifstd.pred(e, Apinatomy.inheritedExternal_s)):
                            layers_ies.append(bindex[layer])
                        else:
                            layers.append(bindex[layer])
                    return layer

        def select(e, m, collect=collect):
            if nifstd.sub(e, m):
                if nifstd.pred(e, Apinatomy.layerIn):
                    # we're at a layer
                    nifstd.ematch(blob, select_term, nifstd.sub(e))
                    return nifstd.ematch(blob, select, nifstd.obj(e))
                elif (nifstd.pred(e, Apinatomy.fasciculatesIn)
                      or nifstd.pred(e, Apinatomy.endsIn)):
                    return nifstd.ematch(blob, select, nifstd.obj(e))
                elif nifstd.pred(e, Apinatomy.ontologyTerms):
                    region = nifstd.obj(e)
                    if region not in doner:
                        doner.add(region)
                        collect.append(bindex[region])
                    return region
        nifstd.ematch(blob, select, nifstd.sub(edge))
        #pprint((collect, layers, layers_ies))
        if collect and not layers and not layers_ies:
            layers = [None]
        elif layers_ies and not layers:
            layers = layers_ies

        # this is a temporary hack, it will go away when inheritedExternals and
        # inheritedOntologyTerms are differentiated in the next release PNS/CNS
        lids = [l['id'] for l in layers if l is not None]
        for bad in EXCLUDED_LAYERS:
            if bad in lids:
                layers = [l for l in layers if l is None or l['id'] != bad]

        # hacked way to not have to deal with layers also matching as regions
        # just remove them from regions if they are in layers ...
        collect = [c for c in collect if c not in layers]
        if len(collect) != len(layers):
            raise ValueError(f'len not matched {[c["id"] for c in collect]} '
                             f'{[l if l is None else l["id"] for l in layers]}\n'
                             f'{edge}')
        #pprint(([c["id"] for c in collect], [l if l is None else l["id"] for l in layers],))
        return list(zip(collect, layers))

    @staticmethod
    def find_terminal_regions(blob, type):
        return [region for es in Apinatomy.find_terminals(blob, type)
                for region in Apinatomy.find_region(blob, es)]

    @staticmethod
    def find_terminal_region_layers(neuron, blob, type, bindex):
        try:
            return [(region, layer) for es in Apinatomy.find_terminals(blob, type)
                    for region, layer in Apinatomy.find_region_layer(blob, es, bindex)]
        except ValueError as err:
            # We've got bad data from SCKAN...
            log.error(f'SCKAN/ApiNATOMY error: {neuron} `{err}`')
            return []

    @staticmethod
    def parse_connectivity(neuron, data):
    #====================================
        def anatomical_layer(pair_list):
            layers = []
            if pair_list[0][0] is None:
                layers.append(pair_list[0][1])
            else:
                layers.extend(pair_list[0])
            for pair in pair_list[1:]:
                layers.extend(pair)
            layers = [layer for layer in layers if layer not in EXCLUDED_LAYERS]
            if len(layers):
                return (layers[0], tuple(layers[1:]))

        blob, *_ = Apinatomy.deblob(data)

        starts = [nifstd.obj(e) for e in blob['edges'] if nifstd.pred(e, Apinatomy.lyphs)]
        nexts = [(nifstd.sub(t), nifstd.obj(t)) for start in starts for t in
                  nifstd.ematch(blob, (lambda e, m: nifstd.pred(e, Apinatomy.next)
                                              or nifstd.pred(e, Apinatomy.next_s)), None)]
        nodes = sorted(set([tuple([Apinatomy.layer_regions(blob, e) for e in p]) for p in nexts]))

        bindex = {n['id']:n for n in blob['nodes']}
        # find terminal regions and layers
        axon_terminal_regions = Apinatomy.find_terminal_region_layers(neuron, blob, Apinatomy.axon, bindex)
        dendrite_terminal_regions = Apinatomy.find_terminal_region_layers(neuron, blob, Apinatomy.dendrite, bindex)

        result = {
            'axons': [al for al in set(anatomical_layer([ ((l['id'] if l is not None else l), r['id']) ])
                        for r, l in axon_terminal_regions) if al is not None],
            'dendrites': [al for al in set(anatomical_layer([ ((l['id'] if l is not None else l), r['id']) ])
                            for r, l in dendrite_terminal_regions) if al is not None],
            'connectivity': [ (al0, al1) for (al0, al1) in set((anatomical_layer(n0[1:][0]), anatomical_layer(n1[1:][0]))
                                for n0, n1 in nodes if n0[1:] != n1[1:] and len(n0[1:][0]) and len(n1[1:][0])) ## This removes self edges... (ICNs)
                                    if al0 is not None and al1 is not None],
        }

        return result

    #===========================================================================

    @staticmethod
    def neuron_knowledge(neuron, data):
    #==================================
        knowledge = {
            'id': neuron,
            'label': neuron
        }
        for node in data['nodes']:
            if node.get('id') == neuron:
                knowledge['label'] = node['meta'].get('synonym', [neuron])[0]
                knowledge['long-label'] = node['lbl']
                break
        apinatomy_neuron = None
        for edge in data['edges']:
            if nifstd.sub(edge, neuron) and nifstd.pred(edge, Apinatomy.annotates):
                apinatomy_neuron = nifstd.obj(edge)
                break
        if apinatomy_neuron is not None:
            references = []
            for edge in data['edges']:
                if nifstd.sub(edge, apinatomy_neuron) and nifstd.pred(edge, Apinatomy.references):
                    references.append(nifstd.obj(edge))
            knowledge['references'] = references
        knowledge.update(Apinatomy.parse_connectivity(neuron, data))
        return knowledge

    @staticmethod
    def model_knowledge(model, data):
    #================================
        # Process result of ``neurons_for_model_cypher(model_id)``
        knowledge = {
            'id': model,
            'paths': []
        }
        for node in data['nodes']:
            if 'Class' in node.get('meta', {}).get('types', []):
                path_id = node['id']
                knowledge['paths'].append({
                    'id': path_id,
                    'models': path_id
                })
        return knowledge

    @staticmethod
    def phenotypes(data):
    #====================
        phenotypes = []
        for edge in data['edges']:
            if edge.get('pred') in [
                    'ilxtr:hasPhenotype',
                    'ilxtr:hasMolecularPhenotype',
                    'ilxtr:hasProjectionPhenotype',
                    'ilxtr:hasCircuitRolePhenotype',
                    'ilxtr:hasFunctionalCircuitRolePhenotype',]:
                phenotypes.append(edge.get('obj'))
        return phenotypes

#===============================================================================
