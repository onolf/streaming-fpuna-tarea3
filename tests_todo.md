# Registro de ejecución de tests

Seguimiento de pruebas ejecutadas sobre `tests/test_assignment.py` como evidencia
de trabajo realizado durante la implementación de `notebook.py`.

## Checklist

- [x] `test_parse_utc_returns_timezone_aware_datetime`
- [x] `test_assign_fixed_window_uses_event_time`
- [x] `test_duplicate_does_not_change_total`
- [x] `test_deduplication_is_isolated_by_merchant`
- [ ] `test_stateful_dofn_keeps_keys_isolated`
- [x] `test_out_of_order_event_uses_its_event_time_window`
- [x] `test_late_event_within_tolerance_is_a_revision`
- [x] `test_event_beyond_lateness_is_audited`
- [ ] `test_windowed_pipeline_produces_totals`
- [ ] `test_trigger_policy_has_lateness_and_accumulating_panes`
- [ ] `test_retries_converge_to_one_materialized_entity`
- [ ] `test_append_only_sink_materializes_every_attempt`
- [x] `test_timer_handler_clears_state`

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

### `test_parse_utc_returns_timezone_aware_datetime`

```
uv run pytest tests/test_assignment.py::test_assign_fixed_window_uses_event_time
=========================================================================================================== test session starts ============================================================================================================
platform linux -- Python 3.12.14, pytest-8.4.2, pluggy-1.6.0
rootdir: /mnt/c/Users/o_nolf.CENTRAL.002/projects/streaming-fpuna-tarea3
configfile: pyproject.toml
plugins: anyio-4.14.2, logfire-4.41.0
collected 1 item

tests/test_assignment.py .                                                                                                                                                                                                           [100%]

============================================================================================================ 1 passed in 0.15s =============================================================================================================
```

**Resultado:** PASSED

### `test_duplicate_does_not_change_total`, `test_late_event_within_tolerance_is_a_revision`, `test_event_beyond_lateness_is_audited`, `test_out_of_order_event_uses_its_event_time_window`, `test_deduplication_is_isolated_by_merchant`

```
$ uv run pytest tests/test_assignment.py::test_duplicate_does_not_change_total tests/test_assignment.py::test_late_event_within_tolerance_is_a_revision tests/test_assignment.py::test_event_beyond_lateness_is_audited tests/test_assignment.py::test_out_of_order_event_uses_its_event_time_window tests/test_assignment.py::test_deduplication_is_isolated_by_merchant -v
=========================================================================================================== test session starts ============================================================================================================
platform linux -- Python 3.12.14, pytest-8.4.2, pluggy-1.6.0 -- /mnt/c/Users/o_nolf.CENTRAL.002/projects/streaming-fpuna-tarea3/.venv/bin/python
cachedir: .pytest_cache
rootdir: /mnt/c/Users/o_nolf.CENTRAL.002/projects/streaming-fpuna-tarea3
configfile: pyproject.toml
plugins: anyio-4.14.2, logfire-4.41.0
collected 5 items

tests/test_assignment.py::test_duplicate_does_not_change_total PASSED                                                                                                                                                                [ 20%]
tests/test_assignment.py::test_late_event_within_tolerance_is_a_revision PASSED                                                                                                                                                      [ 40%]
tests/test_assignment.py::test_event_beyond_lateness_is_audited PASSED                                                                                                                                                               [ 60%]
tests/test_assignment.py::test_out_of_order_event_uses_its_event_time_window PASSED                                                                                                                                                  [ 80%]
tests/test_assignment.py::test_deduplication_is_isolated_by_merchant PASSED                                                                                                                                                          [100%]

============================================================================================================ 5 passed in 0.20s =============================================================================================================
```

**Resultado:** PASSED (5/5). Corregidos previamente 2 bugs en `summarize_payments`: formato ISO de `window_start`/`window_end` (offset `+00:00` en vez de `Z`) y casing de `reason` (`accepted`/`duplicate`/`too_late` en minúsculas).

### `test_timer_handler_clears_state`

```
$ uv run pytest tests/test_assignment.py::test_timer_handler_clears_state
=========================================================================================================== test session starts ============================================================================================================
platform linux -- Python 3.12.14, pytest-8.4.2, pluggy-1.6.0
rootdir: /mnt/c/Users/o_nolf.CENTRAL.002/projects/streaming-fpuna-tarea3
configfile: pyproject.toml
plugins: anyio-4.14.2, logfire-4.41.0
collected 1 item

tests/test_assignment.py .                                                                                                                                                                                                           [100%]

============================================================================================================ 1 passed in 0.09s =============================================================================================================
```

**Resultado:** PASSED. Timer expire handler limpia correctamente el estado de duplicados.