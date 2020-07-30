import os
import logging

import datapackage
from dataflows import Flow, conditional, add_field 
from dgp.config.consts import CONFIG_MODEL_MAPPING, CONFIG_TAXONOMY_CT
from dgp.core import BaseAnalyzer
from dgp_server.log import logger
from dgp_server.publish_flow import append_to_primary_key
from taxonomies.common_transforms import flows as p_flows


COLUMN_MAPPING = dict([
    ('ID_CAPITULO', 'economic-classification:generic:level1:code'),
    ('DESC_CAPITULO', 'economic-classification:generic:level1:label'),
    ('ID_CONCEPTO', 'economic-classification:generic:level2:code'),
    ('DESC_CONCEPTO', 'economic-classification:generic:level2:label'),
    ('ID_PARTIDA_GENERICA', 'economic-classification:generic:level3:code'),
    ('DESC_PARTIDA_GENERICA', 'economic-classification:generic:level3:label'),
    ('ID_PARTIDA_ESPECIFICA', 'economic-classification:generic:level4:code'),
    ('DESC_PARTIDA_ESPECIFICA', 'economic-classification:generic:level4:label'),
])


# class MissingColumns(BaseAnalyzer):

#     def run(self):
#         # Values
#         values = [
#             x
#             for x in self.config.get(CONFIG_TAXONOMY_CT)
#             if x['name'].startswith('value:')
#         ]
#         mapping = self.config.get(CONFIG_MODEL_MAPPING)
#         existing_cts = set(
#             x.get('columnType')
#             for x in mapping
#         )
#         logger.info('EXISTING CTS %r', existing_cts)
#         missing = []
#         for x in values:
#             if x['name'] not in existing_cts:
#                 missing.append(dict(
#                     title=x['title'],
#                     name=x['name'].replace('value:', 'MONTO_'),
#                     columnType=x['name'],
#                     enriched=True,
#                     dataType=x.get('dataType', 'string'),
#                 ))
#         mapping.extend(missing)
#         logger.info('MISSING CTS VALUES %r', missing)

#         # Objeto Del Gasto
#         title_mapping = dict(
#             (v, k) for k, v in COLUMN_MAPPING.items()
#         )
#         missing_cts = [
#             x for x in COLUMN_MAPPING.values()
#             if x not in existing_cts
#         ]
#         missing_cts = [
#             x
#             for x in self.config.get(CONFIG_TAXONOMY_CT)
#             if x['name'] in missing_cts
#         ]
#         missing = [
#             dict(
#                 title=title_mapping[x['name']],
#                 name=title_mapping[x['name']],
#                 columnType=x['name'],
#                 enriched=True,
#                 dataType=x.get('dataType', 'string'),
#             )
#             for x in missing_cts
#         ]
#         logger.info('MISSING CTS OBJETO %r', missing)
#         mapping.extend(missing)
#         self.config.set(CONFIG_MODEL_MAPPING, mapping)


# def missing_types(config):
#     steps = [
#         set_type(
#             m['columnType'].replace(':', '-'),
#             type=m['dataType'],
#             columnType=m['columnType'],
#             **m.get('options', {}),
#         )
#         for m in config.get(CONFIG_MODEL_MAPPING)
#         if m.get('enriched')
#     ]
#     return Flow(*steps)


def objeto_del_gasto(config):

    logging.info('PREPARING objeto_del_gasto processing')

    CT = COLUMN_MAPPING
    CN = dict(
        (k, v.replace(':', '-'))
        for k, v in CT.items()
    )

    lookup = {}
    codes = datapackage.Package(
        os.path.join(os.path.dirname(__file__), 'objeto_del_gasto.datapackage.zip')
    )
    for resource in codes.resources:
        kind = resource.name
        lookup[kind] = {}
        for row in resource.iter(keyed=True):
            key = row[kind.upper().replace('Í', 'I')]
            value = row['DESCRIPCION']
            lookup[kind][key] = value

    def process(row):
        year = int(row['date-fiscal-year'])

        for k, v in CN.items():
            row.setdefault(v, '-' if 'ID_' in k else None)

        # Skip the LAST year of the dataset (currently 2016) it has split columns already
        if year < 2019:
            objeto = row[CN['ID_CONCEPTO']]
            if objeto:
                row[CN['ID_CAPITULO']] = objeto[0] + '000'
                row[CN['ID_CONCEPTO']] = objeto[:2] + '00'
                row[CN['DESC_CAPITULO']] = lookup['capitulo'].get(row[CN['ID_CAPITULO']])
                row[CN['DESC_CONCEPTO']] = lookup['concepto'].get(row[CN['ID_CONCEPTO']])

                nb_generica_digits = 4 if year in (2008, 2009, 2010) else 3

            if objeto and len(objeto) >= 4:
                row[CN['ID_PARTIDA_GENERICA']] = objeto[:nb_generica_digits]

            row[CN['DESC_PARTIDA_GENERICA']] = lookup['partida_generica'].get(row.get(CN['ID_PARTIDA_GENERICA']))

            if year not in (2008, 2009, 2010):
                if objeto and len(objeto) >= 5:
                    row[CN['ID_PARTIDA_ESPECIFICA']] = objeto
                    row[CN['DESC_PARTIDA_ESPECIFICA']] = \
                        lookup['partida_específica'].get(row.get(CN['ID_PARTIDA_ESPECIFICA']))

    def missing_field(f):
        def func(dp):
            return all(f.name != f for f in dp.resources[0].schema.fields)
        return func

    def sort_by_ct():
        def func(package):
            ct_indexes = dict(
                (ct['name'], i)
                for i, ct in enumerate(config.get(CONFIG_TAXONOMY_CT)) 
            )
            fields = sorted((
                (ct_indexes.get(f.get('columnType'), 1000), f)
                for f in package.pkg.descriptor['resources'][0]['schema']['fields']
            ), key=lambda x: x[0])
            package.pkg.descriptor['resources'][0]['schema']['fields'] = [
                f[1] for f in fields
            ]
            yield package.pkg
            yield from package
        return func

    return Flow(
        *[
            conditional(missing_field(CN[f]), Flow(
                add_field(CN[f], 'string', columnType=ct, title=f),
                append_to_primary_key(CN[f]) if 'ID_' in f else None
            ))
            for f, ct in CT.items()
        ],
        sort_by_ct(),
        process
    )


def flows(config, context):
    flows = p_flows(config, context)
    return flows[0], Flow(
        flows[1],
        objeto_del_gasto(config),
    )
