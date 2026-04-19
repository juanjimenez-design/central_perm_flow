"""
This is a boilerplate pipeline 'calac_activos_baj_grad'
generated using Kedro 1.2.0
"""

import pandas as pd
import numpy as np
from datetime import datetime 
import duckdb
import unicodedata
import re
from typing import Tuple



#--------------------------------------------------------------------------------
# Bajas
#--------------------------------------------------------------------------------

def momento_baja(
    central_estaca_sd: pd.DataFrame,
    central_col_fechadef: str,
    central_col_fechatemp:str,
    central_calaca: pd.DataFrame,
    left_on: str,
    right_on: str,
    group_key: str,
    sort_cols: list
) -> pd.DataFrame:
    """
    Alinea los eventos de deserción con la estructura temporal del calendario académico.

    Utiliza una lógica de 'merge_asof' para situar cada fecha de baja dentro del intervalo 
    académico correcto, permitiendo el análisis de eventos en tiempo relativo (T=0).

    Transformaciones principales:
    1.  **Sincronización Temporal**: Vincula cada 'fecha_baja' con la última 'fecha_inicio' 
        disponible mediante búsqueda hacia atrás (backward search).
    2.  **Reinicio de Reloj por Cohorte**: Asegura que la búsqueda se limite a la 
        'cohorte_inicial' del estudiante, respetando su línea de tiempo específica.
    3.  **Gestión de Bajas Prematuras**: Identifica registros con fechas de baja previas 
        al inicio oficial del calendario, categorizándolos como 'Semana 0'.
    4.  **Enriquecimiento de Etapas**: Hereda las métricas de 'month', 'student_journey' 
        y 'mes_label' correspondientes al momento exacto del retiro.

    Args:
        central_bajas: Dataset de estudiantes que presentan evento de deserción.
        central_calaca: Maestro de calendario académico extendido.
        left_on/right_on: Columnas de fecha para la alineación (baja vs. inicio semana).
        group_key: Llave de agrupación (cohorte) para evitar cruces entre periodos distintos.
        sort_cols: Columnas de ordenamiento para garantizar la integridad del merge temporal.

    Returns:
        pd.DataFrame: Dataset de bajas con su ubicación exacta en la cronología académica.
    """
    # 0. Filtrar bajas
    mask_bajas = central_estaca_sd['di'] == 1
    central_bajas = central_estaca_sd[mask_bajas]
    central_bajas['fecha_baja'] = central_bajas[central_col_fechadef]
    central_bajas.loc[central_bajas['fecha_baja'].isna(),'fecha_baja'] = central_bajas.loc[central_bajas['fecha_baja'].isna(),central_col_fechatemp]
    
    # 1. Preparación: Ordenar es obligatorio para merge_asof
    central_bajas = central_bajas.sort_values(left_on)
    central_calaca = central_calaca.sort_values(right_on)

    # 2. Merge Asof: Busca la última 'fecha_inicio' que sea <= 'fecha_baja'
    # 'by' asegura que el tiempo se reinicie/respete por cohorte
    bajas_calac = pd.merge_asof(
        central_bajas,
        central_calaca,
        left_on=left_on,
        right_on=right_on,
        by=group_key,
        direction="backward" # Busca hacia atrás: la semana que ya inició
    )

    # 3. Manejo de bajas "Pre-Calendario"
    # Si la fecha_baja es menor a la primera fecha_inicio, el merge_asof dejará nulos.
    # Los marcamos como 'Semana 0' o 'Mes 0' (Pre-Onboarding)
    pre_calendar_mask = bajas_calac['semana'].isna()
    
    if pre_calendar_mask.any():
        bajas_calac.loc[pre_calendar_mask, 'semana'] = 0
        bajas_calac.loc[pre_calendar_mask, 'month'] = 0
        bajas_calac.loc[pre_calendar_mask, 'student_journey'] = 'pre-onboarding'
        # Asignamos la cohorte_inicial del estudiante para mantener consistencia
        bajas_calac.loc[pre_calendar_mask, 'mes_academico'] = 'm0'

    # 4. Orden final para auditoría
    bajas_calac.sort_values(by= sort_cols, inplace=True)

    return bajas_calac

#---------------------------------------------------------------------------
# Graduados CALAC
#----------------------------------------------------------------------------

def momento_grado(
    central_estaca: pd.DataFrame,
    central_calaca: pd.DataFrame,
    dict_duracion: dict,
    col_gi: str,
    fallback_weeks: int,
    join_left: list,
    join_right: list
) -> pd.DataFrame:
    """
    Sitúa el éxito académico (graduados) en la línea de tiempo del calendario.
    """
    # 1. Filtrar graduados
    graduados = central_estaca[central_estaca[col_gi] == 1].copy()

    # 2. Lógica de asignación de duración teórica
    def get_duration(row):
        nivel = str(row.get('nivel', '')).lower()
        programa = str(row.get('programa', '')).lower()
        
        nivel_cfg = dict_duracion.get(nivel, {})
        # Busca programa específico, si no, default del nivel, si no, fallback global
        prog_cfg = nivel_cfg.get(programa, nivel_cfg.get('default', None))
        
        return prog_cfg['semanas'] if prog_cfg else fallback_weeks

    graduados['max_semana_teorica'] = graduados.apply(get_duration, axis=1)

    # 3. Cruce con calendario para obtener etiquetas temporales (etapa, mes, etc)
    df_graduados_cal = pd.merge(
        graduados,
        central_calaca.drop(columns=['cohorte'], errors='ignore'),
        left_on=join_left,
        right_on=join_right,
        how='left'
    )

    return df_graduados_cal