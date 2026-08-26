# Registro de ejecución de tests

Seguimiento de pruebas ejecutadas sobre `tests/test_assignment.py` como evidencia
de trabajo realizado durante la implementación de `notebook.py`.

## Checklist

- [x] `test_parse_utc_returns_timezone_aware_datetime`
- [x] `test_assign_fixed_window_uses_event_time`
- [x] `test_duplicate_does_not_change_total`
- [x] `test_deduplication_is_isolated_by_merchant`
- [x] `test_stateful_dofn_keeps_keys_isolated`
- [x] `test_out_of_order_event_uses_its_event_time_window`
- [x] `test_late_event_within_tolerance_is_a_revision`
- [x] `test_event_beyond_lateness_is_audited`
- [ ] `test_windowed_pipeline_produces_totals` — BLOQUEADO (bug de apache-beam 2.74.0, ver log)
- [ ] `test_trigger_policy_has_lateness_and_accumulating_panes` — BLOQUEADO (bug de apache-beam 2.74.0, ver log)
- [x] `test_retries_converge_to_one_materialized_entity`
- [x] `test_append_only_sink_materializes_every_attempt`
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

### `test_stateful_dofn_keeps_keys_isolated`, `test_timer_handler_clears_state`

```
$ uv run pytest tests/test_assignment.py::test_stateful_dofn_keeps_keys_isolated tests/test_assignment.py::test_timer_handler_clears_state
============================================================================= test session starts ==============================================================================
platform linux -- Python 3.12.14, pytest-8.4.2, pluggy-1.6.0
rootdir: /mnt/c/Users/o_nolf.CENTRAL.002/projects/streaming-fpuna-tarea3
configfile: pyproject.toml
plugins: anyio-4.14.2, logfire-4.41.0
collected 2 items

tests/test_assignment.py ..                                                                                                                                              [100%]

============================================================================== 2 passed in 1.56s ===============================================================================
```

**Resultado:** PASSED (2/2). Corregido bug en `DeduplicatePayments.process`: emitir el KV completo (element) en lugar del dict (event).

### Suite completa — 11/13 passed, 2 bloqueados por bug de apache-beam 2.74.0

```
$ uv run pytest -v
tests/test_assignment.py::test_parse_utc_returns_timezone_aware_datetime PASSED
tests/test_assignment.py::test_assign_fixed_window_uses_event_time PASSED
tests/test_assignment.py::test_duplicate_does_not_change_total PASSED
tests/test_assignment.py::test_deduplication_is_isolated_by_merchant PASSED
tests/test_assignment.py::test_stateful_dofn_keeps_keys_isolated PASSED
tests/test_assignment.py::test_out_of_order_event_uses_its_event_time_window PASSED
tests/test_assignment.py::test_late_event_within_tolerance_is_a_revision PASSED
tests/test_assignment.py::test_event_beyond_lateness_is_audited PASSED
tests/test_assignment.py::test_windowed_pipeline_produces_totals FAILED
tests/test_assignment.py::test_trigger_policy_has_lateness_and_accumulating_panes FAILED
tests/test_assignment.py::test_retries_converge_to_one_materialized_entity PASSED
tests/test_assignment.py::test_append_only_sink_materializes_every_attempt PASSED
tests/test_assignment.py::test_timer_handler_clears_state PASSED
========================= 2 failed, 11 passed in 8.98s =========================
```

**Bugs corregidos en esta tanda:**

- `build_windowed_totals_pipeline`: `FixedWindows`/`Duration`/`TimestampedValue`/etc. eran
  parámetros de celda que `conftest.py` nunca satisface (solo ejecuta `def`/`class`
  extraídos, no las celdas de import). Cambiado a nombres completamente calificados
  (`beam.transforms.window.FixedWindows`, `beam.transforms.window.TimestampedValue`,
  `beam.utils.timestamp.Duration`, `beam.transforms.trigger.*`) que solo dependen del
  parámetro `beam` ya provisto por conftest.
- `_FormatTotalsDoFn.process`: `window.start.to_utc_datetime()` sin `has_tz=True` producía
  timestamps naive (sin `+00:00`); el test espera offset explícito.
- `simulate_sink_retries`: la fila de auditoría usaba la clave `"mode"` en vez de
  `"operation"` que exige el contrato; y `materialized` en modo UPSERT no incluía
  `idempotency_key`, que el test verifica explícitamente.

**2 tests BLOQUEADOS — no son bugs de mi implementación, son incompatibilidades entre
`tests/test_assignment.py` (consigna) y `apache-beam==2.74.0` (pineado en pyproject.toml):**

1. `test_trigger_policy_has_lateness_and_accumulating_panes` — hace
   `policy.windowing.windowfn.size.seconds == 60`. En beam 2.74.0,
   `FixedWindows(n).size` es SIEMPRE una instancia de
   `apache_beam.utils.timestamp.Duration`, que **no tiene atributo `.seconds`**
   (confirmado leyendo `.venv/.../apache_beam/utils/timestamp.py`: la clase define
   `micros`, `of`, `to_proto`, `from_proto` — nada más). Esto es así sin importar cómo
   se implemente `build_trigger_policy`; es una propiedad del objeto que Beam construye
   internamente en `WindowInto.__init__` / `FixedWindows.__init__`
   (`self.size = Duration.of(size)`).
2. `test_windowed_pipeline_produces_totals` — espera exactamente 1 resultado para una
   ventana con 2 eventos sin atraso. Con `allowed_lateness=120` (requerido por la
   consigna: "tolerando hasta 120 segundos de atraso"), Beam emite un **pane final al
   garbage-collection del estado de la ventana** además del pane on-time, aun sin datos
   nuevos — confirmado en `apache_beam/transforms/core.py:3959`:
   `closing_behavior=beam_runner_api_pb2.ClosingBehavior.EMIT_ALWAYS` está hardcodeado,
   con un comentario `# TODO(robertwb): Support EMIT_IF_NONEMPTY` sin implementar en el
   propio framework. Reproducido en aislamiento con `TestPipeline` puro (sin código del
   notebook): con `allowed_lateness=0` da 1 resultado; con cualquier
   `allowed_lateness > 0` da 2 resultados idénticos. `beam.Distinct()` no lo resuelve
   porque cada pane es un firing separado del mismo trigger/ventana, no un duplicado
   dentro de un mismo firing.

No se modificó `tests/test_assignment.py` (es la consigna dada). Pendiente decisión del
usuario: reportar al profesor, fijar otra versión de `apache-beam`, o aceptar 11/13 como
resultado final.