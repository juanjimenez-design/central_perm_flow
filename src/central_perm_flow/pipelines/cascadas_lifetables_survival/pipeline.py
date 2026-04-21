"""
This is a boilerplate pipeline 'cascadas_lifetables_survival'
generated using Kedro 1.2.0
"""

from kedro.pipeline import Node, Pipeline, node, pipeline
from .nodes import crear_cascada_supervivencia,calcular_km_y_eti_dinamico


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        node(
                func=crear_cascada_supervivencia,
                inputs={
                    "central_estados_calac": "central_estados_calac",
                    "central_calendario_extendido_uptoday": "central_calendario_extendido_uptoday",
                    "central_bajas_calendario_academico": "central_bajas_calendario_academico",
                    "central_graduados_calendario_academico": "central_graduados_calendario_academico",
                    "dict_niveles_duracion": "params:survival.dict_niveles_duracion",
                    "params": "params:survival"  # <-- Esta llave debe ser igual al nombre en el def de la función
                },
                outputs="cascadas_semanal_podada_censuras",
                name="nodo_integracion_cascada_supervivencia",
            ),
            node(
                func=calcular_km_y_eti_dinamico,
                inputs={
                    "df": "cascadas_semanal_podada_censuras",
                    "group_cols": "params:survival.group_columnas_agrupacion"
                },
                outputs="central_tabla_vida",
                name="nodo_calculo_km_eti_final",
            )
    ])
