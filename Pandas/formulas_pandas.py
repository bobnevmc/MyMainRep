import sys

import pandas as pd
import numpy as np
import math
from datetime import datetime
import snowflake.connector
import sys

import snowflake_1 as SF1

import creds as MC


def sf_pd_fetchall(
        conn_id: str,
        query: Union[str, Path],
        column_transform: Optional[Callable[[str], str]] = lambda col: col.upper(),
        data_transform: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        query_params: Optional[Dict] = None,
        params: Optional[Dict] = None
) -> pd.DataFrame:
    """
    Выполняет SQL-запрос в Snowflake и возвращает результаты в формате pandas DataFrame.

    Аргументы:
        conn_id (str):
            Идентификатор подключения к Snowflake.
        query (Union[str, Path]):
            SQL-запрос в виде строки или пути к файлу, содержащему SQL-запрос.
        column_transform (Optional[Callable], optional):
            Функция для обработки названий колонок. По умолчанию преобразует названия колонок
            в верхний регистр. Принимает строку (имя колонки) и возвращает обработанную строку.
            Если `None`, названия колонок остаются неизменными.
        data_transform (Optional[Callable], optional):
            Функция для обработки данных в DataFrame. Принимает DataFrame и
            возвращает измененный DataFrame. Если `None`, данные остаются неизменными.
        params (Optional[Dict], optional):
            Параметры для выполнения SQL-запроса. Используется для параметризованных запросов.

    Возвращает:
        pd.DataFrame:
            DataFrame с результатами SQL-запроса. Названия колонок и/или данные
            могут быть обработаны в зависимости от переданных функций.

    Пример использования:
        # Выполнение запроса с обработкой названий колонок
        df = sf_pd_fetchall(
            conn_id="my_snowflake_conn",
            query="SELECT * FROM my_table",
            column_transform=lambda col: col.lower().replace(" ", "_")
        )

        # Выполнение запроса с кастомной обработкой данных
        df = sf_pd_fetchall(
            conn_id="my_snowflake_conn",
            query="SELECT * FROM my_table",
            data_transform=lambda df: df[df['value'] > 10]
        )

        # Передача запроса из файла
        df = sf_pd_fetchall(
            conn_id="my_snowflake_conn",
            query=Path("/path/to/query.sql")
        )
    """

    # Проверяем, является ли query путем к файлу
    if os.path.isfile(query):
        with open(query, "r", encoding="utf-8") as file:
            query = file.read()
            log.info("Query loaded from file.")
    else:
        log.info("Query provided as string.")

    if query_params is not None:
        query = query.format(**query_params)

    # Подключение к Snowflake
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    db_uri = hook.get_uri()
    log.info(f"Executing query: {query}")

    # Выполнение запроса
    df = pd.read_sql_query(query, db_uri, params=params)
    log.info("The query has been successfully executed.")

    if df.empty:
        raise EmptyQueryResult()
    log.info(f"DataFrame size: {df.shape[0]} rows, {df.shape[1]} columns.")

    # Изменение названий колонок
    if column_transform is not None:
        if not callable(column_transform):
            raise ValueError("'column_transform' must be a callable function or None.")
        df.columns = [column_transform(col) for col in df.columns]
        log.info("Column names transformed.")

    # Преобразование данных
    if data_transform is not None:
        if not callable(data_transform):
            raise ValueError("data_transform must be a callable function or None.")
        df = data_transform(df)
        log.info("Data transformed using the provided function.")

    return df