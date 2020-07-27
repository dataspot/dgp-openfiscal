import dataflows as DF

opts = dict(infer_strategy=DF.load.INFER_STRINGS)

DF.Flow(
    DF.load('data/objeto_del_gasto/capitulo.csv', **opts),
    DF.load('data/objeto_del_gasto/concepto.csv', **opts),
    DF.load('data/objeto_del_gasto/partida_específica.csv', **opts),
    DF.load('data/objeto_del_gasto/partida_generica.csv', **opts),
    DF.printer(),
    DF.dump_to_zip('objeto_del_gasto.datapackage.zip')
).process()
