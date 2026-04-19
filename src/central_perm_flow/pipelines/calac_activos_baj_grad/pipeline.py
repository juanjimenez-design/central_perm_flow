"""
This is a boilerplate pipeline 'calac_activos_baj_grad'
generated using Kedro 1.2.0
"""


from kedro.pipeline import Node, Pipeline, node, pipeline
from .nodes import momento_baja,momento_grado



def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [node(
            func=momento_baja,
            inputs={
                "central_estaca_sd": "central_estaca_sd",
                "central_col_fechadef": "params:bajas_calac.central_col_fechadef",
                "central_col_fechatemp": "params:bajas_calac.central_col_fechatemp",
                "central_calaca": "central_calendario_extendido",
                "left_on": "params:bajas_calac.merge_left_on", # 'fecha_baja'
                "right_on": "params:bajas_calac.merge_right_on", # 'fecha_inicio'
                "group_key": "params:bajas_calac.central_calaca_col_cohorte_inicial",
                "sort_cols": "params:bajas_calac.central_calaca_col_sort",
            },
            outputs="bajas_calendario_academico",
            name="node_momento_baja"
        ),
        node(
                func=momento_grado,
                inputs={
                    "central_estaca": "central_estaca_sd",
                    "central_calaca": "central_calendario_extendido",
                    "dict_duracion": "params:graduados_calac.dict_niveles_duracion",
                    "col_gi": "params:graduados_calac.graduation_col_gi",
                    "fallback_weeks": "params:graduados_calac.graduation_fallback_weeks",
                    "join_left": "params:graduados_calac.graduation_join_keys_left",
                    "join_right": "params:graduados_calac.graduation_join_keys_right",
                },
                outputs="graduados_calendario_academico",
                name="node_momento_graduacion"
            )
    ]
    )
