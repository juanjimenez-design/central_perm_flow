"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 1.2.0
"""

from kedro.pipeline import Node, Pipeline, node, pipeline
from .nodes import central_preprocessing_estaca,central_preprocessing_calaca

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [   node(
                func=central_preprocessing_calaca,
                inputs={
                    "central_calendario": "central_calaca",
                    "col_fechas": "params:data_processing_calaca.central_calaca_col_fechas",
                    "col_fechaingreso": "params:data_processing_calaca.central_calaca_col_fechaingreso",
                    "col_fecha_ini_sem": "params:data_processing_calaca.central_calaca_col_fecha_inicial_semana",
                    "col_fecha_fin_sem": "params:data_processing_calaca.central_calaca_col_fecha_final_semana",
                    "col_cohorte_ini": "params:data_processing_calaca.central_calaca_col_cohorte_inicial",
                    "col_sort": "params:data_processing_calaca.central_calaca_col_sort",
                    "journey_labels": "params:data_processing_calaca.central_calalaca_student_journey",
                    "journey_thresholds": "params:data_processing_calaca.central_calaca_journey_thresholds",
                    "col_ordenadas": "params:data_processing_calaca.central_calaca_column_order"
                },
                outputs=["central_calendario_extendido","central_calendario_extendido_uptoday"],
                name="node_preprocessing_calaca"
            ),
            node(
                func=central_preprocessing_estaca,
                inputs={
                    "central_estaca": "central_estaca", # Nombre de la tabla en el catálogo
                    "central_col_fechas": "params:data_processing_estaca.central_col_fechas",
                    "central_col_emails": "params:data_processing_estaca.central_col_emails",
                    "central_col_dd": "params:data_processing_estaca.central_col_dd",
                    "central_col_sort": "params:data_processing_estaca.central_col_sort",
                    "central_niveles_academicos": "params:data_processing_estaca.central_niveles_academicos",
                    "central_estaca_column_order": "params:data_processing_estaca.central_estaca_column_order",
                },
                outputs=["central_estaca_sd", "central_estaca_cd"],
                name="node_central_preprocessing_estaca",
            ),
        ]
    )