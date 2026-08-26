import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es un esqueleto. Las celdas de código contienen firmas,
    contratos y excepciones `NotImplementedError`; no incluyen la solución.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        if not isinstance(raw_value, str):
            raise ValueError(f"Expected string, got {type(raw_value).__name__}")
        if not raw_value.endswith("Z"):
            raise ValueError(f"Timestamp must end with 'Z' (UTC), got: {raw_value}")
        # Replace Z with +00:00 for fromisoformat compatibility
        normalized = raw_value[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(normalized)
        except ValueError as e:
            raise ValueError(f"Invalid ISO-8601 timestamp: {raw_value}") from e

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        if not isinstance(timestamp, datetime):
            raise ValueError(f"Expected datetime, got {type(timestamp).__name__}")

        # Obtener epoch seconds usando el método nativo de datetime
        ts_seconds = timestamp.timestamp()

        # Calcular el inicio de la ventana alineado al tamaño
        window_start_seconds = int(ts_seconds // size_seconds * size_seconds)

        # Reconstruir datetimes preservando la zona horaria original
        tz = timestamp.tzinfo
        window_start = datetime.fromtimestamp(window_start_seconds, tz=tz)
        window_end = datetime.fromtimestamp(window_start_seconds + size_seconds, tz=tz)

        return window_start, window_end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.
        """
        from datetime import datetime, timezone

        # Estructura para acumular totales: {(merchant_id, window_start, window_end): total}
        accumulated_totals = {}

        # Estructuras auxiliares para control (duplicados, etc.)
        seen_events_per_merchant = set()
        audit_list = []

        # Filtrar solo los eventos confirmados
        confirmed_events = [
            event
            for event in events
            if event.get("status") == "CONFIRMED"
        ]

        # Parsear timestamps y ordenar eventos por event_time para procesamiento determinista
        parsed_events = []
        for event in confirmed_events:
            try:
                event_time_dt = parse_utc(event["event_time"])
                arrival_time_dt = parse_utc(event["arrival_time"])
                event_time_ts = event_time_dt.timestamp()
                arrival_time_ts = arrival_time_dt.timestamp()
                parsed_events.append({
                    **event,
                    "_event_time_dt": event_time_dt,
                    "_arrival_time_dt": arrival_time_dt,
                    "_event_time_ts": event_time_ts,
                    "_arrival_time_ts": arrival_time_ts,
                })
            except (KeyError, ValueError) as e:
                # Si falta event_time o arrival_time, o son inválidos, saltar evento
                continue

        sorted_events = sorted(
            parsed_events,
            key=lambda x: (x["_event_time_ts"], x.get("event_id", ""))
        )

        # Calcular max_event_time para la marca de agua global
        max_event_time = max((e["_event_time_ts"] for e in sorted_events), default=0)

        # Calcular la marca de agua (watermark) global basada en el evento más avanzado visto
        watermark = max_event_time - allowed_lateness_seconds

        for event in sorted_events:
            event_id = event.get("event_id")
            merchant_id = event.get("merchant_id")
            event_time_ts = event["_event_time_ts"]
            arrival_time_ts = event["_arrival_time_ts"]
            event_time_dt = event["_event_time_dt"]

            delay_seconds = arrival_time_ts - event_time_ts

            # Verificar duplicados
            duplicate = False
            if deduplicate:
                event_key = (merchant_id, event_id)
                if event_key in seen_events_per_merchant:
                    duplicate = True
                else:
                    seen_events_per_merchant.add(event_key)

            # Calcular ventana de tiempo usando event_time
            window_start_dt, window_end_dt = assign_fixed_window(event_time_dt, window_seconds)
            window_start_ts = window_start_dt.timestamp()
            window_end_ts = window_end_dt.timestamp()

            # Verificar si es demasiado tarde (too_late)
            # too_late si event_time < watermark O si arrival_time > window_end + allowed_lateness
            too_late = False
            if event_time_ts < watermark or arrival_time_ts > (window_end_ts + allowed_lateness_seconds):
                too_late = True

            # Determinar si se acepta
            accepted = not duplicate and not too_late

            # Determinar si hay revisión (llega tarde pero dentro del margen permitido de latencia)
            # revision = accepted y arrival_time > window_end (llegó después del cierre de ventana pero dentro de allowed_lateness)
            revision = False
            if accepted and arrival_time_ts > window_end_ts:
                revision = True

            reason = "accepted"
            if duplicate:
                reason = "duplicate"
            elif too_late:
                reason = "too_late"

            if accepted:
                key = (merchant_id, window_start_ts, window_end_ts)
                amount = event.get("amount", 0)
                accumulated_totals[key] = accumulated_totals.get(key, 0) + amount

            audit_list.append({
                "event_id": event_id,
                "merchant_id": merchant_id,
                "delay_seconds": delay_seconds,
                "duplicate": duplicate,
                "too_late": too_late,
                "accepted": accepted,
                "revision": revision,
                "reason": reason,
            })

        # Construir la lista de totales con formato ISO-8601
        totals_list = []
        for (merchant_id, w_start_ts, w_end_ts), total in sorted(
            accumulated_totals.items(),
            key=lambda x: (x[0][0], x[0][1])
        ):
            dt_start = datetime.fromtimestamp(w_start_ts, tz=timezone.utc).isoformat()
            dt_end = datetime.fromtimestamp(w_end_ts, tz=timezone.utc).isoformat()
            totals_list.append({
                "merchant_id": merchant_id,
                "window_start": dt_start,
                "window_end": dt_end,
                "total": total,
            })

        return totals_list, audit_list

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usar Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """
        class _FormatTotalsDoFn(beam.DoFn):
            """Formatear totales por comercio agregando metadatos de ventana."""

            def process(
                self,
                element: tuple[str, float],
                window=beam.DoFn.WindowParam,
            ):
                merchant_id, total = element
                yield {
                    "merchant_id": merchant_id,
                    "window_start": window.start.to_utc_datetime(has_tz=True).isoformat(),
                    "window_end": window.end.to_utc_datetime(has_tz=True).isoformat(),
                    "total": total,
                }

        # Paso 1: crear la PCollection inicial a partir de la lista de eventos
        # Paso 2: filtrar únicamente los pagos con estado CONFIRMED
        # Paso 3: asignar el timestamp de event_time usando TimestampedValue
        timestamped_events = (
            pipeline
            | "CreateEvents" >> beam.Create(events)
            | "FilterConfirmed" >> beam.Filter(
                lambda event: event.get("status") == "CONFIRMED"
            )
            | "AttachEventTime" >> beam.Map(
                lambda event: beam.transforms.window.TimestampedValue(
                    event, parse_utc(event["event_time"]).timestamp()
                )
            )
        )

        # Paso 4: aplicar ventana fija con lateness permitido y triggers acumulativos
        windowed_events = (
            timestamped_events
            | "WindowInto" >> beam.WindowInto(
                beam.transforms.window.FixedWindows(window_seconds),
                allowed_lateness=beam.utils.timestamp.Duration(seconds=120),
                accumulation_mode=beam.transforms.trigger.AccumulationMode.ACCUMULATING,
            )
        )

        # Paso 5: emitir pares (merchant_id, amount) y agregar con CombinePerKey
        # Paso 6: proyectar el resultado con los límites de ventana vía WindowParam
        totals_by_window = (
            windowed_events
            | "KeyByMerchant" >> beam.Map(
                lambda event: (event["merchant_id"], event.get("amount", 0))
            )
            | "SumPerMerchant" >> beam.CombinePerKey(sum)
            | "FormatTotals" >> beam.ParDo(_FormatTotalsDoFn())
        )

        return totals_by_window

    return


@app.cell
def _(Any, SetStateSpec, StrUtf8Coder, TimeDomain, TimerSpec, beam, on_timer):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            _merchant_id, event = element
            event_id = event["event_id"]

            # Si el event id ya fue visto en esta ventana, descartar el duplicado
            if event_id in seen_ids.read():
                return

            # Registrar el event id como visto y re-emitir el par (clave, evento)
            seen_ids.add(event_id)
            # Programar el timer para limpiar el estado cuando termine la ventana.
            # El watermark no alcanzará window.end hasta window.end + allowed lateness,
            # momento en que ya no se aceptarán más eventos tardíos para esta ventana.
            expiry.set(window.end)
            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """
        # Componer el trigger:
        # - on-time: el pane principal se dispara cuando el watermark cruza el
        #   final de la ventana (cálculo confirmado del total).
        # - early: estimaciones periódicas basadas en processing time (5s) para
        #   disponer de resultados parciales antes del cierre de la ventana.
        # - late: revisiones periódicas cada 10s de processing time, que
        #   re-emiten el pane cuando llega un evento aceptado dentro de la
        #   ventana de lateness permitida.
        # - accumulation_mode ACCUMULATING: cada pane tardío contiene el total
        #   acumulado hasta ese momento, no sólo el delta; esto es coherente
        #   con la lógica de CombinePerKey(sum) y permite que un downstream
        #   idempotente reciba totales consistentes para reemplazar filas.
        trigger = beam.transforms.trigger.AfterWatermark(
            early=beam.transforms.trigger.AfterProcessingTime(5),
            late=beam.transforms.trigger.AfterProcessingTime(10),
        )

        return beam.WindowInto(
            beam.transforms.window.FixedWindows(window_seconds),
            allowed_lateness=beam.utils.timestamp.Duration(seconds=allowed_lateness_seconds),
            trigger=trigger,
            accumulation_mode=beam.transforms.trigger.AccumulationMode.ACCUMULATING,
        )

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        # La clave idempotente identifica de forma única un resultado lógico:
        # combina el comercio con el inicio de su ventana para que un reintento
        # apunte exactamente a la misma entidad en el sink.
        merchant_id = result["merchant_id"]
        window_start = result["window_start"]
        return f"{merchant_id}|{window_start}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
        # Estructuras internas del sink según el modo de operación.
        # POST append-only usa una lista; UPSERT idempotente usa un dict
        # indexado por la clave lógica para que reintentos sobrescriban.
        audit: list[dict[str, Any]] = []

        if idempotent:
            # Modo UPSERT idempotente: el segundo intento del mismo resultado
            # reemplaza la fila existente en lugar de duplicarla.
            upsert_sink: dict[str, dict[str, Any]] = {}
            for attempt_number in range(1, attempts + 1):
                for result_row in results:
                    idempotency_key = make_idempotency_key(result_row)
                    audit.append({
                        "attempt": attempt_number,
                        "mode": "UPSERT",
                        "idempotency_key": idempotency_key,
                        "row": result_row,
                    })
                    # La sobrescritura por clave garantiza una sola entidad final.
                    upsert_sink[idempotency_key] = result_row
            # El estado visible del sink son los valores almacenados por clave.
            materialized = list(upsert_sink.values())
        else:
            # Modo POST append-only: cada intento genera una fila nueva
            # aunque la clave lógica sea la misma.
            append_sink: list[dict[str, Any]] = []
            for attempt_number in range(1, attempts + 1):
                for result_row in results:
                    idempotency_key = make_idempotency_key(result_row)
                    audit.append({
                        "attempt": attempt_number,
                        "mode": "POST",
                        "idempotency_key": idempotency_key,
                        "row": result_row,
                    })
                    # El append siempre acumula una fila adicional.
                    append_sink.append(result_row)
            materialized = append_sink

        return materialized, audit

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. Implementá las
    funciones hasta que estas garantías queden verdes:

    - [ ] un duplicado no modifica el total;
    - [ ] claves distintas no comparten estado;
    - [ ] un evento fuera de orden cae en su ventana de evento;
    - [ ] un evento con atraso aceptado produce una revisión;
    - [ ] un evento demasiado tardío queda auditado;
    - [ ] dos escrituras del mismo resultado dejan una sola entidad;
    - [ ] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
