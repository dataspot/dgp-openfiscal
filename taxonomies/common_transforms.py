from dataflows import Flow, update_package, conditional

from dgp.core.base_enricher import enrichments_flows, BaseEnricher
from dgp.config.consts import RESOURCE_NAME, CONFIG_PRIMARY_KEY
from dgp_server.log import logger


class LoadMetadata(BaseEnricher):

    def test(self):
        return self.config._unflatten().get('extra', {}).get('metadata')

    def postflow(self):
        metadata = self.config._unflatten().get('extra', {}).get('metadata')
        logger.info('UPDATING WITH METADATA %r', metadata)
        return Flow(
            update_package(**metadata)
        )


class Deduplicator(BaseEnricher):

    def test(self):
        return True

    def dedup(self, pkg):
        def func(rows):
            if rows.res.name == RESOURCE_NAME:
                used = set()
                key_field_names = rows.res.descriptor['schema']['primaryKey']
                logger.info('DEDPULICATING with KEYS %r', key_field_names)
                for row in rows:
                    key = tuple(row.get(k) for k in key_field_names)
                    if key not in used:
                        used.add(key)
                        yield row
            else:
                yield from rows
        return Flow(func)


    def postflow(self):
        steps = [
            conditional(lambda pkg: True,
                        lambda pkg: self.dedup(pkg)),
        ]
        f = Flow(*steps)
        return f


# class Deduplicator(BaseEnricher):

#     def test(self):
#         logger.info('DEDPULICATING %r', self.config.get('extra.deduplicate'))
#         return self.config.get('extra.deduplicate')

#     def postflow(self):
#         key_field_names = [
#             ct.replace(':', '-')
#             for ct in self.config.get(CONFIG_PRIMARY_KEY)
#         ]
#         value_field_names = [
#             mapping['columnType'].replace(':', '-')
#             for mapping in self.config.get(CONFIG_MODEL_MAPPING)
#             if ('columnType' in mapping and
#                 mapping['columnType'].split(':')[0] == 'value')
#         ]
#         steps = [
#             join_with_self(
#                 RESOURCE_NAME,
#                 key_field_names,
#                 {
#                     **dict((f, {}) for f in key_field_names),
#                     **dict((f, dict(aggregate='sum')) for f in value_field_names),
#                     '*': dict(aggregate='last')
#                 }
#             ),
#         ]
#         logger.info('DEDPULICATING with KEYS %r', key_field_names)
#         f = Flow(*steps)
#         return f


def flows(config, context):
    return enrichments_flows(
        config, context,
        Deduplicator,
        LoadMetadata,
    )
