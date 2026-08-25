# Registro de ejecución de tests

Seguimiento de pruebas ejecutadas sobre `tests/test_assignment.py` como evidencia
de trabajo realizado durante la implementación de `notebook.py`.

## Checklist

- [x] `test_parse_utc_returns_timezone_aware_datetime`
- [ ] `test_assign_fixed_window_uses_event_time`
- [ ] `test_duplicate_does_not_change_total`
- [ ] `test_deduplication_is_isolated_by_merchant`
- [ ] `test_stateful_dofn_keeps_keys_isolated`
- [ ] `test_out_of_order_event_uses_its_event_time_window`
- [ ] `test_late_event_within_tolerance_is_a_revision`
- [ ] `test_event_beyond_lateness_is_audited`
- [ ] `test_windowed_pipeline_produces_totals`
- [ ] `test_trigger_policy_has_lateness_and_accumulating_panes`
- [ ] `test_retries_converge_to_one_materialized_entity`
- [ ] `test_append_only_sink_materializes_every_attempt`
- [ ] `test_timer_handler_clears_state`

## Log de ejecuciones

### `test_parse_utc_returns_timezone_aware_datetime`

```
$ uv run pytest tests/test_assignment.py::test_parse_utc_returns_timezone_aware_datetime
=========================================================================================================== test session starts ============================================================================================================
platform linux -- Python 3.12.14, pytest-8.4.2, pluggy-1.6.0
rootdir: /mnt/c/Users/o_nolf.CENTRAL.002/projects/streaming-fpuna-tarea3
configfile: pyproject.toml
plugins: anyio-4.14.2, logfire-4.41.0
collected 1 item

tests/test_assignment.py .                                                                                                                                                                                                           [100%]

============================================================================================================ 1 passed in 0.23s =============================================================================================================
```

**Resultado:** PASSED
