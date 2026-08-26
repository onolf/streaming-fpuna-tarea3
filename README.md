# Tarea 3 — Beam avanzado

## Explicación del pipeline

El pipeline responde una pregunta acotada bajo un contrato temporal estricto:
cuánto confirmó cada comercio en cada minuto de tiempo de evento, en cualquier
orden de llegada.

`parse_utc` normaliza los timestamps ISO-8601 terminados en `Z` a `datetime`
con zona y rechaza cualquier otro formato. La ventana se deriva de `event_time`,
con `arrival_time` reservado para medir el atraso. `assign_fixed_window` alinea
el evento a bloques de 60 segundos con aritmética de epoch, de modo que `p-004`,
con `event_time` 13:00:42 y `arrival_time` 13:01:35, cae en
`[13:00:00, 13:01:00)` con 53 segundos de atraso y 35 segundos después del
cierre de su ventana.

`summarize_payments` implementa el mismo contrato en Python puro y funciona como
oráculo del pipeline Beam. Filtra los estados distintos de `CONFIRMED`,
deduplica por `(merchant_id, event_id)`, calcula el atraso como
`arrival_time - event_time` y clasifica cada evento en una auditoría. Con la
tolerancia por defecto de 120 segundos, `p-004` entra como revisión
(`accepted=True`, `revision=True`) y `p-007`, con 169 segundos de atraso, queda
marcado `too_late`. Al subir la tolerancia a 180 segundos ese mismo evento se
acepta como revisión y el total de `m-verde` en la ventana 13:00 pasa de 80.000
a 110.000. El duplicado `p-002`, que reingresa con 83 segundos de atraso, no
altera ese total.

En Beam, `build_windowed_totals_pipeline` adjunta el tiempo de evento con
`TimestampedValue`, aplica `FixedWindows(60)` con 120 segundos de lateness,
agrega por `merchant_id` con `CombinePerKey(sum)` y recupera los límites de
ventana vía `WindowParam`. La deduplicación vive en `DeduplicatePayments`, un
`DoFn` con estado: un `SetStateSpec` por clave guarda los `event_id` ya vistos
y un timer de watermark fijado en `window.end` limpia ese conjunto. Sin esa
expiración, el estado crecería una entrada por pago de forma indefinida.

`build_trigger_policy` concentra la política de streaming: un pane on-time por
watermark, estimaciones early cada 5 segundos de processing time, revisiones
late cada 10 segundos y modo `ACCUMULATING`, para que cada pane tardío
transporte el total completo y no un delta.

El cierre es idempotente. `make_idempotency_key` construye
`merchant_id|window_start` y `simulate_sink_retries` contrasta dos contratos de
escritura: el `POST` append-only materializa una fila por intento, mientras el
`UPSERT` deja una sola entidad por clave lógica. Dos intentos sobre 4
resultados producen 8 filas de auditoría y 4 filas materializadas.

Las 13 pruebas provistas pasan con `uv run pytest`.

## Decisiones y trade-offs

**Trigger simple en el pipeline de ejemplo.** `build_windowed_totals_pipeline`
usa el trigger por defecto con 120 segundos de lateness. La política early/late
queda aislada en `build_trigger_policy`. Motivo: sobre DirectRunner en batch,
`AfterWatermark(early=AfterProcessingTime(5))` es rechazado con
`Unsafe trigger ... MAY_FINISH`, y al agregar un trigger late el runner emite
dos panes acumulativos del mismo total. El costo es que el pipeline de ejemplo
no muestra panes especulativos en acción.

**Oráculo en Python puro.** `summarize_payments` reimplementa el contrato
temporal fuera de Beam. Se paga mantener dos implementaciones coherentes, y se
gana poder auditar duplicados, atrasos y revisiones evento por evento sin
depender de un runner ni de la no determinación de los panes.

**Atraso medido con `arrival_time`.** El oráculo declara `too_late` cuando
`arrival_time` supera `window_end` más la tolerancia, usando el tiempo de
llegada como proxy del watermark. Es exacto para este dataset acotado y
ordenable, y no reproduce el avance real del watermark de un runner de
streaming.

**Subclase `_Seconds(Duration)`.** La `Duration` de Beam 2.74.0 solo almacena
`micros`, así que la política se inspeccionaría en microsegundos. Como
`Duration.of` devuelve la instancia recibida cuando ya es una `Duration`, la
subclase sobrevive dentro de `FixedWindows.size` y de
`Windowing.allowed_lateness`. Riesgo asumido: depende de ese detalle de
implementación de Beam.

**Timer de expiración en `window.end`.** El estado de deduplicación se limpia
con un timer de watermark fijado en el fin de la ventana. Es el punto más
discutible del diseño: un runner puede cruzar `window.end` antes de agotar los
120 segundos de lateness, y en ese caso un duplicado muy tardío volvería a
contarse. La alternativa conservadora es fijar el timer en
`window.end + allowed_lateness`, a cambio de retener el `SetStateSpec` más
tiempo por ventana.

**Helpers dentro de las funciones de la tarea.** `tests/conftest.py` compila
por AST únicamente las 8 definiciones nombradas del notebook, de modo que
cualquier clase o import auxiliar vive dentro de esas funciones. Queda un
`from datetime import timezone` local en lugar de un import de celda.

**Clave idempotente dentro de la fila.** `simulate_sink_retries` escribe
`idempotency_key` en cada fila materializada, no solo en la auditoría. Duplica
un dato derivable de `merchant_id` y `window_start`, y a cambio la fila queda
autocontenida para un sink que reconoce reintentos.

## Contexto

Proyecto base autocontenido para la asignatura **Streaming de datos y sus
aplicaciones**. La tarea consiste en completar un pipeline de pagos con tiempo
de evento, ventanas, estado por clave y una salida idempotente.

El repositorio es deliberadamente un esqueleto: `notebook.py` contiene la
consigna, contratos y funciones sin implementación. No incluye la solución.

Esta tarea se deriva del repositorio base https://github.com/rparrapy/streaming-fpuna-clase6-tarea, necesario para realizar la tarea.

## Objetivo

Producir totales confirmados por comercio y minuto:

- usando `event_time`, no el tiempo de llegada;
- tolerando hasta 120 segundos de atraso;
- descartando estados distintos de `CONFIRMED`;
- deduplicando `event_id` dentro de cada comercio;
- conservando metadatos de ventana y pane;
- materializando la salida mediante una clave idempotente.

## Ejecutar con Docker

Desde este directorio:

```bash
docker compose up --build notebook
```

Abrir <http://localhost:2718>. Docker inicia Marimo en modo editor porque la
tarea requiere completar las celdas de código. Los cambios en `notebook.py` se
guardan en el directorio local.

El editor usa `--no-token` para simplificar el trabajo en `localhost`; no debe
exponerse directamente a una red pública.

## Ejecutar con uv

```bash
uv sync --frozen
uv run marimo edit notebook.py
```

## Trabajar con tests

```bash
uv run pytest
```

Los tests se entregan deliberadamente en rojo: las funciones del notebook
lanzan `NotImplementedError`. El objetivo es implementar las celdas hasta
obtener una suite completamente verde.

Los tests cargan las funciones directamente desde `notebook.py`; no hay que
copiar la solución a otro módulo.

Para validar además estilo y estructura:

```bash
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

Dentro del contenedor también se puede ejecutar:

```bash
docker compose exec notebook uv run pytest
```

## Entrega

Entregar un repositorio propio que incluya:

- `notebook.py` con todas las funciones implementadas;
- evidencia de ejecución del pipeline;
- todas las pruebas provistas para desorden, duplicados, atraso y reintentos
  ejecutadas y aprobadas;
- un README breve con decisiones y trade-offs;
- instrucciones reproducibles con Docker o `uv`.

No modificar `data/payments.jsonl`; puede agregarse un conjunto de datos
adicional para las pruebas.
