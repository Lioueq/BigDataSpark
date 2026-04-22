# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

## Цель работы

Реализовать ETL-пайплайн на Spark с загрузкой данных из staging в модель звезда PostgreSQL и построением 6 обязательных витрин в ClickHouse.

## Что сделано

1. Реализован Spark job `raw_to_star.py`:
2. Реализован Spark job `star_to_clickhouse_reports.py`:
	- формирование 6 витрин: `report_products`, `report_customers`, `report_time`, `report_stores`, `report_suppliers`, `report_quality`

## Запуск

```bash
docker compose up
```

## Построить звезду в PostgreSQL:

```bash
podman compose exec spark spark-submit --jars /home/jovyan/work/jars/postgresql-42.7.4.jar /home/jovyan/work/jobs/raw_to_star.py
```

## Построить витрины в ClickHouse:

```bash
podman compose exec spark env CH_USER=default CH_PASSWORD=clickhouse spark-submit --jars /home/jovyan/work/jars/postgresql-42.7.4.jar,/home/jovyan/work/jars/clickhouse-jdbc-0.7.2-all.jar /home/jovyan/work/jobs/star_to_clickhouse_reports.py
```
