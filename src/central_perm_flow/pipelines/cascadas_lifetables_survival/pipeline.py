from kedro.pipeline import Pipeline, node, pipeline
from .nodes import crear_cascada_supervivencia, calcular_km_y_eti_dinamico

def create_pipeline(**kwargs) -> Pipeline:
    # 1. Definimos el pipeline "maestro" de cálculo actuarial que será reutilizado
    calculo_base = pipeline([
        node(
            func=calcular_km_y_eti_dinamico,
            inputs={
                "df": "cascadas_semanal_podada_censuras",
                "group_cols": "params:group_columnas_agrupacion" # Nombre relativo al namespace
            },
            outputs="central_tabla_vida", # Nombre relativo al namespace
            name="nodo_calculo_km_eti",
        )
    ])

    return Pipeline([
        # --- NODO DE INTEGRACIÓN (Se ejecuta una sola vez) ---
        node(
            func=crear_cascada_supervivencia,
            inputs={
                "central_estados_calac": "central_estados_calac",
                "central_calendario_extendido_uptoday": "central_calendario_extendido_uptoday",
                "central_bajas_calendario_academico": "central_bajas_calendario_academico",
                "central_graduados_calendario_academico": "central_graduados_calendario_academico",
                "dict_niveles_duracion": "params:survival.dict_niveles_duracion",
                "params": "params:survival"
            },
            outputs="cascadas_semanal_podada_censuras",
            name="nodo_integracion_cascada_supervivencia",
        ),

        # --- INSTANCIAS CON NAMESPACE ---
        # Mapeamos el input global "cascadas_semanal_podada_censuras" a cada namespace
        
        pipeline(
            calculo_base,
            namespace="lifetables_programa",
            inputs={"cascadas_semanal_podada_censuras": "cascadas_semanal_podada_censuras"}
        ),
        
        pipeline(
            calculo_base,
            namespace="lifetables_nivel",
            inputs={"cascadas_semanal_podada_censuras": "cascadas_semanal_podada_censuras"}
        ),
        
        pipeline(
            calculo_base,
            namespace="lifetables_nivel_academico",
            inputs={"cascadas_semanal_podada_censuras": "cascadas_semanal_podada_censuras"}
        ),
        
        pipeline(
            calculo_base,
            namespace="lifetables_cohorte",
            inputs={"cascadas_semanal_podada_censuras": "cascadas_semanal_podada_censuras"}
        ),
    ])