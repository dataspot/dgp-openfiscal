import os
import copy
import json

from sqlalchemy import create_engine

from slugify import slugify

from dataflows import Flow, add_field, conditional, validate, schema_validator, update_package, finalizer
from dataflows_normalize import normalize_to_db, NormGroup 

from dgp.core.base_enricher import enrichments_flows, BaseEnricher
from dgp.core import BaseDataGenusProcessor
from dgp.config.log import logger
from dgp.config.consts import (
    CONFIG_URL,
    CONFIG_TAXONOMY_CT, CONFIG_TAXONOMY_ID,
    CONFIG_PRIMARY_KEY,
    RESOURCE_NAME
)

from dgp_server.publish_flow import publish_flow, append_to_primary_key, clear_by_source, get_source

from taxonomies.babbage_models import babbage_models

db_connection_string = os.environ['DATASETS_DATABASE_URL']
engine = create_engine(db_connection_string)


CONFIG_EXTRA_METADATA_DATASET_NAME = 'extra.metadata.dataset-name'
CONFIG_EXTRA_METADATA_REVISION = 'extra.metadata.revision'
CONFIG_EXTRA_RESOURCE_NAME = 'extra.resource-name'
CONFIG_EXTRA_PRIVATE = 'extra.private'
CONFIG_EXTRA_METADATA_TITLE = 'extra.metadata.title'



class AppendMetadata(BaseEnricher):

    def test(self):
        return True

    def postflow(self):
        metadata = self.config._unflatten().get('extra', {}).get('metadata', {})
        return Flow(
            add_field('metadata', 'object', metadata)
        )


class MissingColumnsAdder(BaseEnricher):

    def test(self):
        return True

    def no_such_field(self, field_name):
        def func(dp):
            ret = all(field_name != f.name for f in dp.resources[0].schema.fields)
            if ret:
                print('Adding missing field for {}'.format(field_name))
            return ret
        return func

    def postflow(self):
        steps = []
        for ct in self.config.get(CONFIG_TAXONOMY_CT):
            name = ct['name'].replace(':', '-')
            dataType = ct['dataType']
            unique = ct.get('unique')
            if unique:
                flow = Flow(
                    add_field(name, dataType, '-', resources=RESOURCE_NAME, columnType=ct['name']),
                    append_to_primary_key(name)
                )
            else:
                flow = Flow(
                    add_field(name, dataType, None, resources=RESOURCE_NAME, columnType=ct['name']),
                )
            steps.append(
                conditional(
                    self.no_such_field(name),
                    flow
                )
            )
        return Flow(*steps)


class DropInvalidRows(BaseEnricher):

    def test(self):
        return True

    def postflow(self):
        return Flow(
            validate(on_error=schema_validator.drop),
        )



class OSPublisherDGP(BaseEnricher):

    def test(self):
        return True

    def ref_column(self, prefix):
        return '{}_id'.format(prefix)

    def id_column(self):
        return 'id'

    def slugify(self, x):
        return slugify(x, separator='_')

    def column(self, x):
        return x.replace(':', '-')

    def fetch_label(self, code_ct, mapping):
        label_ct = None
        for ct in self.config.get(CONFIG_TAXONOMY_CT):
            if ct.get('labelOf'):
                if code_ct.startswith(ct['labelOf']):
                    label_ct = ct['name']
                    break
        if label_ct is not None:
            for m in mapping:
                if m.get('columnType') == label_ct:
                    return m

    def fetch_datatype(self, ct):
        for item in self.config.get(CONFIG_TAXONOMY_CT):
            if item['name'] == ct:
                return item.get('dataType', 'string')

    def normalize(self, package, full_name, db_table):
        schema = package.descriptor['resources'][0]['schema']
        fields = schema['fields']
        print(json.dumps(fields, indent=2))
        primary_key = schema['primaryKey']
        mapping = []
        for f in fields:
            m = copy.deepcopy(f) 
            if m.get('columnType'):
                m['slug'] = self.slugify(m['title'])
                m['hierarchy'] = self.slugify(m['columnType'].split(':')[0])
                m['column'] = self.column(m['columnType'])
                m['primaryKey'] = m['name'] in primary_key
                m['measure'] = m['hierarchy'] == 'value'
                m['full_column'] = (
                    m['column'] if m['measure']
                    else '{}_{hierarchy}.{column}'.format(db_table, **m)
                )
                m['label'] = self.fetch_label(m['columnType'], mapping)
                m['dataType'] = self.fetch_datatype(m['columnType'])
                mapping.append(m)
        prefixes = set(
            m['hierarchy']
            for m in mapping
            if m.get('measure') is False
        )
        prefixed = dict(
            (p, list(filter(lambda m: m.get('hierarchy') == p, mapping)))
            for p in prefixes
        )
        print(json.dumps(prefixed, indent=2))
        groups = [
            NormGroup([
                    m['column']
                    for m in prefixed_items
                ], self.ref_column(prefix), self.id_column(),
                db_table='{}_{}'.format(db_table, prefix))
            for prefix, prefixed_items in prefixed.items()
        ]
        babbage_model = dict(
            dimensions=dict(
                (m['slug'], dict(
                    label=m['title'],
                    key_attribute=m['slug'],
                    attributes=dict([
                        (m['slug'], dict(
                            column=m['full_column'],
                            label=m['title'],
                            type=m['dataType'],
                        ))
                    ] + ([
                        (m['label']['slug'], dict(
                            column=m['label']['full_column'],
                            label=m['label']['title'],
                            type=m['label']['dataType'],
                        ))
                    ] if m.get('label') else [])),
                    join_column=[
                        self.ref_column(m['hierarchy']),
                        self.id_column()
                    ],
                    **(dict(
                        label_attribute=m['label']['slug']
                    ) if m.get('label') else {})
                ))
                for m in mapping
                if m.get('measure') is False and m.get('primaryKey') is True
            ),
            fact_table=db_table,
            measures=dict(
                (
                    m['slug'],
                    dict(
                        column=m['column'],
                        label=m['title'],
                        type='number'
                    )
                )
                for m in mapping
                if m.get('measure') is True
            ),
            hierarchies=dict(
                (prefix, dict(
                    label=prefix,
                    levels=[
                        m['slug']
                        for m in prefixed_items
                        if m.get('primaryKey') is True
                    ]
                ))
                for prefix, prefixed_items in prefixed.items()
            ),
        )

        return Flow(
            update_package(babbage_model=babbage_model),
            normalize_to_db(
                groups,
                db_table,
                RESOURCE_NAME,
                db_connection_string,
                'append'
            ),
            finalizer(lambda: babbage_models.create_or_edit(full_name, babbage_model))
        )

    def postflow(self):
        steps = []
        logger.info('Publisher Flow Preparing')

        full_name = '{}_{}'.format(
            self.config.get(CONFIG_TAXONOMY_ID),
            slugify(self.config.get(CONFIG_EXTRA_METADATA_DATASET_NAME), separator='_', lowercase=True),
        )
        db_table = 'dgp__{}'.format(full_name)
        source = get_source(self.config)
        steps.extend([
            add_field('_source', 'string', source),
            append_to_primary_key('_source'),
            clear_by_source(engine, db_table, source),
            conditional(lambda pkg: True, lambda pkg: self.normalize(pkg, full_name, db_table)),
        ])

        logger.info('Publisher Flow Prepared')
        return Flow(*steps)




def flows(config, context):
    return enrichments_flows(
        config, context,
        MissingColumnsAdder,
        DropInvalidRows,
        OSPublisherDGP,
    )
