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
    central_col_fechatemp: str,
    dict_duracion: dict,
    fallback_weeks: int,
    central_calaca: pd.DataFrame,
    left_on: str,
    right_on: str,
    group_key: str,
    sort_cols: list
) -> pd.DataFrame:
    
    """
    Sincroniza eventos de deserción con la estructura temporal del calendario.

    Lógica:
    1. Filtra desertores (di=1) y consolida la fecha de baja (definitiva o temporal).
    2. Estandariza precisión de tiempo (ns) y ordena datos para 'merge_asof'.
    3. Vincula cada baja con la última semana iniciada (backward) según su cohorte.
    4. Normaliza como 'pre-onboarding' (Semana 0) los retiros previos al calendario.

    Args:
        central_estaca_sd: Dataset maestro de estudiantes.
        central_col_fechadef/temp: Nombres de columnas de fecha para consolidación.
        central_calaca: Maestro de calendario académico.
        left_on, right_on, group_key: Parámetros de alineación temporal y por cohorte.
        sort_cols: Columnas para el ordenamiento final de auditoría.

    Returns:
        pd.DataFrame: desertores con ubicación exacta en la cronología académica.
    """

    # 0. Filtrar bajas usando .copy() para evitar SettingWithCopyWarning
    mask_bajas = central_estaca_sd['di'] == 1
    central_bajas = central_estaca_sd[mask_bajas].copy()
    
    # Consolidación de fecha de baja (Definitiva o Temporal)
    central_bajas['fecha_baja'] = central_bajas[central_col_fechadef].fillna(central_bajas[central_col_fechatemp])

    # --- MEJORA DE ESTABILIDAD: Tipos y Precisión ---
    central_bajas[left_on] = pd.to_datetime(central_bajas[left_on]).dt.as_unit('ns')
    central_calaca[right_on] = pd.to_datetime(central_calaca[right_on]).dt.as_unit('ns')
    
    # 1. Preparación: Ordenar es obligatorio para merge_asof
    central_bajas = central_bajas.sort_values(left_on)
    central_calaca = central_calaca.sort_values(right_on)

    # 2. max_fecha_teorica
        # 2. Lógica de duración teórica
    def get_duration(row):
        nivel = str(row.get('nivel', '')).lower()
        programa = str(row.get('programa', '')).lower()
        nivel_cfg = dict_duracion.get(nivel, {})
        prog_cfg = nivel_cfg.get(programa, nivel_cfg.get('default', None))
        return prog_cfg['semanas'] if prog_cfg else fallback_weeks

    central_bajas['max_semana_teorica'] = central_bajas.apply(get_duration, axis=1)

    # 2. Merge Asof
    bajas_calac = pd.merge_asof(
        central_bajas,
        central_calaca.drop(columns=['cohorte'], errors='ignore'), # Evitamos duplicar cohorte
        left_on=left_on,
        right_on=right_on,
        by=group_key,
        direction="backward" 
    )

    # 3. Manejo de bajas "Pre-Calendario"
    pre_calendar_mask = bajas_calac['semana'].isna()
    
    if pre_calendar_mask.any():
        bajas_calac.loc[pre_calendar_mask, 'semana'] = 0
        bajas_calac.loc[pre_calendar_mask, 'semana_acumulada'] = 0
        bajas_calac.loc[pre_calendar_mask, 'month'] = 0
        bajas_calac.loc[pre_calendar_mask, 'student_journey'] = 'pre-onboarding'
        bajas_calac.loc[pre_calendar_mask, 'mes_academico'] = 'm0'

    # 4. Orden final para auditoría
    bajas_calac.sort_values(by=sort_cols, inplace=True)

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
    Sitúa graduados en el calendario académico y asigna metas de duración teórica.

    Lógica:
    1. Filtra registros con éxito académico (gi=1).
    2. Asigna duración teórica esperada según nivel y programa (benchmarking).
    3. Cruza con el calendario para obtener etiquetas temporales (mes, etapa, journey).

    Args:
        central_estaca, central_calaca: Datasets de estudiantes y calendario.
        dict_duracion, fallback_weeks: Configuración para el cálculo de semanas teóricas.
        col_gi, join_left, join_right: Columna de grado y llaves para el cruce de datos.

    Returns:
        pd.DataFrame: Graduados con métricas de tiempo y ubicación en la cronología.
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

#---------------------------------------------------------------------------
# Activos CALAC
#----------------------------------------------------------------------------

def momento_activos(
    central_estaca: pd.DataFrame,
    central_calaca: pd.DataFrame,
    dict_duracion: dict,
    col_di: str,
    col_gi: str,
    fallback_weeks: int,
    join_left: str,   
    join_right: str,  
    group_key: str
) -> pd.DataFrame:
    """
    Sitúa estudiantes activos en el calendario académico e identifica rezagos (ENGI).

    Lógica:
    1. Filtra activos (di=0, gi=0) y cruza con calendario según fecha actual.
    2. Calcula duración teórica por programa vs. progreso real.
    3. Clasifica como ENGI (1) a quienes superan el tiempo previsto.
    4. Normaliza como 'pre-onboarding' (Semana 0) ingresos futuros o sin registro.

    Args:
        central_estaca, central_calaca: Datasets de estudiantes y calendario.
        dict_duracion, fallback_weeks: Configuración de tiempos teóricos.
        col_di, col_gi, group_key: Columnas de estado y agrupación por cohorte.
        join_left, join_right: Llaves temporales para el cruce asincrónico.

    Returns:
        pd.DataFrame: Activos con métricas de journey y bandera ENGI.
    """
    # 1. Filtrar activos y estandarizar tiempos
    central_activos = central_estaca[
        (central_estaca[col_di] == 0) & (central_estaca[col_gi] == 0)
    ].copy()
    
    # Generamos fecha actual normalizada y aseguramos tipos
    central_activos[join_left] = pd.to_datetime(pd.Timestamp.now()).normalize()
    central_activos[join_left] = central_activos[join_left].astype('datetime64[ns]')
    central_calaca[join_right] = central_calaca[join_right].astype('datetime64[ns]')
    
    central_activos = central_activos.sort_values(join_left)
    central_calaca = central_calaca.sort_values(join_right)

    # 2. Lógica de duración teórica
    def get_duration(row):
        nivel = str(row.get('nivel', '')).lower()
        programa = str(row.get('programa', '')).lower()
        nivel_cfg = dict_duracion.get(nivel, {})
        prog_cfg = nivel_cfg.get(programa, nivel_cfg.get('default', None))
        return prog_cfg['semanas'] if prog_cfg else fallback_weeks

    central_activos['max_semana_teorica'] = central_activos.apply(get_duration, axis=1)

    # 3. Cruce con calendario
    df_activos_cal = pd.merge_asof(
        central_activos,
        central_calaca.drop(columns=['cohorte'], errors='ignore'),
        left_on=join_left,
        right_on=join_right,
        by=group_key,
        direction="backward"
    )

    # 4. LIMPIEZA: Manejo de Pre-onboarding y Nulos
    # Definimos la máscara: fecha futura O si el merge no encontró semana (NaN)
    pre_calendar_mask = ( 
        (df_activos_cal['semana'].isna())
    )
    
    if pre_calendar_mask.any():
        df_activos_cal.loc[pre_calendar_mask, 'semana'] = 0
        df_activos_cal.loc[pre_calendar_mask, 'semana_acumulada'] = 0
        df_activos_cal.loc[pre_calendar_mask, 'month'] = 0
        df_activos_cal.loc[pre_calendar_mask, 'mes_academico'] = 'm0'
        df_activos_cal.loc[pre_calendar_mask, 'student_journey'] = 'pre-onboarding'

    # 5. Indicadora ENGI
    # Al haber limpiado los nulos a 0, esta comparación ya no fallará
    df_activos_cal['engi'] = (
        df_activos_cal['semana_acumulada'] >= df_activos_cal['max_semana_teorica']
    ).astype(int)
    
    # 6. Truncar la semana acumulada
    mask_truncar =  df_activos_cal['semana_acumulada'] >= df_activos_cal['max_semana_teorica']
    df_activos_cal.loc[mask_truncar,'semana_acumulada'] = df_activos_cal['max_semana_teorica']

    # 7. Indicador de activos 
    df_activos_cal['ai'] = 1
    df_activos_cal.loc[df_activos_cal['engi']==1, 'ai'] = 0

    return df_activos_cal



#---------------------------------------------------------------------------
# Consolidación Población Académica
#----------------------------------------------------------------------------



def consolidar_universo_academico(
    df_bajas: pd.DataFrame,
    df_graduados: pd.DataFrame,
    df_activos: pd.DataFrame,
    col_orden : list
) -> pd.DataFrame:
    """
    Concatena los tres estados académicos y normaliza indicadores.
    """
    # 1. Unimos los tres dataframes
    universo = pd.concat([df_bajas, df_graduados, df_activos], ignore_index=True)
    
    # 2. Normalizamos la columna engi: si es nula (viene de bajas/grados), es 0
    if 'engi' in universo.columns:
        universo['engi'] = universo['engi'].fillna(0).astype(int)
    if 'ai' in universo.columns:
        universo['ai'] = universo['ai'].fillna(0).astype(int)
    if 'di' in universo.columns:
        universo['di'] = universo['di'].fillna(0).astype(int)
    if 'gi' in universo.columns:
        universo['gi'] = universo['di'].fillna(0).astype(int)

    # 2. Censuras
    universo['ci'] = universo['gi'] + universo['engi']

    # 3. Orden de las columnas
    universo = universo.loc[:,col_orden]
    return universo