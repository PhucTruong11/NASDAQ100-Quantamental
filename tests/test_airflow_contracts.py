"""Run in the Airflow image; local environments without Airflow skip these tests."""
import importlib.util
import json
import os
import unittest
from unittest.mock import Mock, patch
from pathlib import Path

HAS_AIRFLOW = os.name == 'posix' and importlib.util.find_spec('airflow') is not None
ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(HAS_AIRFLOW, 'Airflow contracts require the Airflow runtime')
class AirflowContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from airflow.models import DagBag
        cls.bag = DagBag(str(ROOT / 'dags'), include_examples=False)
        cls.transform = cls.bag.get_dag('dbt_transformation_pipeline')
        cls.ingest = cls.bag.get_dag('nasdaq100_ingestion_pipeline')

    def test_no_import_errors(self):
        self.assertEqual(self.bag.import_errors, {})

    def test_gold_precedes_final_quality_gate(self):
        chain = ['dbt_run_bronze', 'dbt_run_silver', 'dbt_run_intermediate',
                 'dbt_build_gold', 'dbt_test_quality_gates', 'audit_backtest_inputs']
        for i, task_id in enumerate(chain):
            self.assertEqual(self.transform.get_task(task_id).upstream_task_ids,
                             {chain[i-1]} if i else set())

    def test_single_warehouse_pool_and_no_duplicate_var_option(self):
        for task in self.transform.tasks:
            self.assertEqual(task.pool, 'duckdb_access')
            if task.task_id.startswith('dbt_'):
                self.assertIn('--vars "$QUANT_DBT_VARS"', task.bash_command)
                self.assertNotIn('--var ', task.bash_command)
        self.assertEqual(self.transform.max_active_runs, 1)
        self.assertEqual(self.bag.get_dag('test_duckdb_connect').tasks[0].pool, 'duckdb_access')

    def test_boolean_mode_is_forwarded_in_conf_and_env(self):
        trigger = self.ingest.get_task('trigger_dbt_transformation')
        gold = self.transform.get_task('dbt_build_gold')
        for mode in [False, True]:
            context = {'params': {'allow_observed_universe': mode}}
            conf = trigger.render_template(trigger.conf, context, self.ingest.get_template_env())
            self.assertIs(conf['allow_observed_universe'], mode)
            env = gold.render_template(gold.env, context, self.transform.get_template_env())
            self.assertIs(json.loads(env['QUANT_DBT_VARS'])['allow_observed_universe'], mode)

    def test_strict_default_and_string_booleans_rejected(self):
        from airflow.exceptions import ParamValidationError
        for dag in [self.ingest, self.transform]:
            param = dag.params.get_param('allow_observed_universe')
            self.assertIs(param.resolve(), False)
            with self.assertRaises(ParamValidationError):
                param.resolve(value='true')

    def test_parent_waits_and_propagates_child_failure(self):
        trigger = self.ingest.get_task('trigger_dbt_transformation')
        self.assertTrue(trigger.wait_for_completion)
        self.assertIn('failed', trigger.failed_states)
        self.assertEqual(trigger.retries, 0)
        self.assertNotEqual(trigger.pool, 'duckdb_access')

    def test_sector_task_accepts_partial_fresh_write_but_rejects_stale_file(self):
        import polars as pl
        task=self.ingest.get_task('fetch_gics_sector')
        adapter=Mock();adapter.run.return_value=False
        adapter.last_fetched_count=9;adapter.failed_tickers=['MISSING']
        with patch('ingestion.adapters.sector.SectorAdapter',return_value=adapter), \
             patch('polars.read_parquet',return_value=pl.DataFrame({'Ticker':['A']})), \
             patch('pathlib.Path.exists',return_value=True), \
             patch('pathlib.Path.stat',return_value=Mock(st_size=10)):
            adapter.last_write_succeeded=False
            with self.assertRaises(RuntimeError):task.python_callable()
            adapter.last_write_succeeded=True
            task.python_callable()


if __name__ == '__main__':
    unittest.main()
