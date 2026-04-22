"""
This is a boilerplate pipeline 'calac_activos_baj_grad'
generated using Kedro 1.2.0
"""


from kedro.pipeline import Node, Pipeline, node, pipeline
from .nodes import momento_baja,\
                   momento_grado,\
                   momento_activos,\
                   consolidar_estados_calac



def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [node(
            func=momento_baja,
            inputs={
                "central_estaca_sd": "central_estaca_sd",
                "central_col_fechadef": "params:bajas_calac.central_col_fechadef",
                "central_col_fechatemp": "params:bajas_calac.central_col_fechatemp",
                "dict_duracion": "params:graduados_calac.dict_niveles_duracion",
                "central_calaca": "central_calendario_extendido_uptoday",
                "fallback_weeks": "params:graduados_calac.graduation_fallback_weeks",
                "left_on": "params:bajas_calac.merge_left_on", # 'fecha_baja'
                "right_on": "params:bajas_calac.merge_right_on", # 'fecha_inicio'
                "group_key": "params:bajas_calac.central_calaca_col_cohorte_inicial",
                "sort_cols": "params:bajas_calac.central_calaca_col_sort",
            },
            outputs="central_bajas_calendario_academico",
            name="node_momento_baja"
        ),
        node(
                func=momento_grado,
                inputs={
                    "central_estaca": "central_estaca_sd",
                    "central_calaca": "central_calendario_extendido_uptoday",
                    "dict_duracion": "params:graduados_calac.dict_niveles_duracion",
                    "col_gi": "params:graduados_calac.graduation_col_gi",
                    "fallback_weeks": "params:graduados_calac.graduation_fallback_weeks",
                    "join_left": "params:graduados_calac.graduation_join_keys_left",
                    "join_right": "params:graduados_calac.graduation_join_keys_right",
                },
                outputs="central_graduados_calendario_academico",
                name="node_momento_graduacion"
            ),
        node(
                func=momento_activos,
                inputs={
                    "central_estaca": "central_estaca_sd",              # Dataset maestro
                    "central_calaca": "central_calendario_extendido_uptoday",   # Maestro calendario
                    "dict_duracion": "params:graduados_calac.dict_niveles_duracion", # Reutilizamos dict
                    "col_di": "params:activos_calac.col_di",            # 'di'
                    "col_gi": "params:activos_calac.col_gi",            # 'gi'
                    "fallback_weeks": "params:graduados_calac.graduation_fallback_weeks",
                    "join_left": "params:activos_calac.join_keys_left",  # 'fecha_activo'
                    "join_right": "params:activos_calac.join_keys_right",# 'fecha_inicio'
                    "group_key": "params:activos_calac.group_key",       # 'cohorte_inicial'
                },
                outputs="central_activos_calendario",
                name="nodo_momento_activos",
            ),
            node(
            func=consolidar_estados_calac,
            inputs=[
                "central_bajas_calendario_academico",
                "central_graduados_calendario_academico",
                "central_activos_calendario",
                "params:orden_columnas_universo",
            ],
            outputs="central_estados_calac",
            name="nodo_consolidar_estados_calac"
        ),
    ]
    )
