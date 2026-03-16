"""Polling orchestrator.

Manages periodic data fetching from the Databricks and Spark APIs,
supporting both fast (jobs/stages) and slow (cluster/events) refresh intervals.
"""
