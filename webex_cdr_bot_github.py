#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bot Webex (WebSockets) + CDR Bot (48h)
- Comando:  cdr start=<fecha> end=<fecha> [locations=a,b] [csv] [excel] [rows=20]
- Sin parámetros → envía Adaptive Card para completar Inicio/Fin/Locations.
- Tokens hardcodeados arriba (NO subir a git).

Requisitos:
  pip install webex-bot webexpythonsdk requests pandas openpyxl
"""

import os
import re
import sys
import csv
import tempfile
import requests
import datetime as dt
from typing import Dict, List, Optional, Iterator
import os

WEBEX_ACCESS_TOKEN=YWM2Yzg3ODItZGY1OC00MzJlLWE5ZGYtZWE0NjNkMjNlZTBlNDQ1NmNlYzEtNzAw_PF84_8c3c67f5-9dba-4be8-b058-76d7afe45521
WEBEX_ADMIN_TOKEN=Y2FlMGQ4N2ItMTQ2MS00M2NhLTgxZWQtMWU4MTBkNDI4MmYyM2E2YWQxODAtNzk4_PF84_8c3c67f5-9dba-4be8-b058-76d7afe45521

if not WEBEX_ACCESS_TOKEN:
    raise RuntimeError("Falta WEBEX_ACCESS_TOKEN")

if not WEBEX_ADMIN_TOKEN:
    raise RuntimeError("Falta WEBEX_ADMIN_TOKEN")

print("TOKEN:", bool(os.getenv("WEBEX_ACCESS_TOKEN")))
# ========= Webex Bot (WebSockets) =========
from webex_bot.webex_bot import WebexBot
from webex_bot.models.command import Command
from webex_bot.models.response import response_from_adaptive_card
from webexpythonsdk import WebexAPI

# Tarjetas (usar OBJETOS del SDK; no dicts)
from webexpythonsdk.models.cards import (
    AdaptiveCard, TextBlock, ColumnSet, Column,
    FontWeight, FontSize, Colors, HorizontalAlignment, Text
)
from webexpythonsdk.models.cards.actions import Submit

ANALYTICS_BASE = "https://analytics.webexapis.com/v1/cdr_feed"

# ========= Zona horaria local (Argentina) =========
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    AR_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
except Exception:
    AR_TZ = dt.timezone(dt.timedelta(hours=-3))  # Argentina sin DST

# ========= Utilidades CDR =========

def parse_link_header(link_header: str) -> Dict[str, str]:
    """Devuelve {rel: url} desde el header HTTP Link."""
    rels: Dict[str, str] = {}
    if not link_header:
        return rels
    parts = [p.strip() for p in link_header.split(",") if p.strip()]
    for part in parts:
        if ";" not in part:
            continue
        url_part, *params = part.split(";")
        url = url_part.strip().lstrip("<").rstrip(">")
        rel_value = None
        for param in params:
            if "=" not in param:
                continue
            k, v = param.split("=", 1)
            if k.strip().lower() == "rel":
                rel_value = v.strip().strip('"')
        if url and rel_value:
            rels[rel_value] = url
    return rels


def parse_local_or_iso(s: str) -> dt.datetime:
    """Acepta ISO-8601 con o sin zona. Si no hay tz, asume AR (-03:00). Devuelve UTC (aware)."""
    s_orig = s.strip()
    if not s_orig:
        raise ValueError("datetime vacío")

    s = s_orig
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    # agrega :ss si faltan
    def _add_seconds_if_missing(x: str) -> str:
        tpos = x.find('T') if 'T' in x else x.find(' ')
        if tpos == -1:
            return x
        rest = x[tpos+1:]
        tzpos_rel = None
        for marker in ['Z', '+', '-']:
            p = rest.find(marker)
            if p != -1:
                tzpos_rel = p
                break
        if tzpos_rel is not None:
            time_part = rest[:tzpos_rel]
            tz_part = rest[tzpos_rel:]
        else:
            time_part = rest
            tz_part = ''
        if time_part and time_part.count(':') == 1:
            return x[:tpos+1] + time_part + ':00' + tz_part
        return x

    s = _add_seconds_if_missing(s)

    try:
        d = dt.datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=AR_TZ)
        return d.astimezone(dt.timezone.utc)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            d = dt.datetime.strptime(s_orig, fmt).replace(tzinfo=AR_TZ)
            return d.astimezone(dt.timezone.utc)
        except ValueError:
            continue

    raise ValueError(f"No pude interpretar la fecha/hora: '{s_orig}'")


def validate_feed_window(start: dt.datetime, end: dt.datetime):
    now_utc = dt.datetime.now(dt.timezone.utc)
    if end <= start:
        raise ValueError("Fin debe ser posterior a Inicio.")
    if (now_utc - end) < dt.timedelta(minutes=5):
        raise ValueError("Fin debe ser ≥ 5 minutos antes de ahora.")
    if (now_utc - start) > dt.timedelta(hours=48):
        raise ValueError("Inicio no puede ser más antiguo que 48 horas.")


def feed_iter(token: str, start: dt.datetime, end: dt.datetime,
              locations_param: Optional[List[str]] = None) -> Iterator[Dict]:
    """Itera sobre todos los items del CDR Bot (48h)."""
    params = {
        "startTime": start.isoformat().replace("+00:00", "Z"),
        "endTime": end.isoformat().replace("+00:00", "Z"),
    }
    if locations_param:
        params["locations"] = ",".join(locations_param)

    url = ANALYTICS_BASE
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    while url:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized: token inválido o sin scope spark-admin:calling_cdr_read.")
        if r.status_code == 403:
            raise RuntimeError("403 Forbidden: privilegios insuficientes para Detailed Call History API.")
        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text}")

        data = r.json()
        for item in data.get("items", []):
            yield item

        link = parse_link_header(r.headers.get("Link", ""))
        url = link.get("next")
        params = None  # la next URL ya trae la query completa


def csv_headers_union(rows: List[Dict]) -> List[str]:
    headers: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                headers.append(k)
    return headers


def write_csv_all_fields(path: str, rows: List[Dict]):
    """CSV con todas las columnas presentes en los registros."""
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["Start Time", "Location", "Calling Number", "Called Number", "Duration"])
        return
    headers = csv_headers_union(rows)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def to_excel(csv_path: str, xlsx_path: str):
    try:
        import pandas as pd
    except ModuleNotFoundError:
        raise RuntimeError("Para XLSX instalá: pip install pandas openpyxl")
    df = pd.read_csv(csv_path)
    df.to_excel(xlsx_path, index=False)


# ========= Tabla resumen Markdown =========

SUMMARY_COLS = ["Start Time", "Location", "Called Number", "Duration", "Direction"]

def _canon(x: str) -> str:
    return "".join(ch.lower() for ch in x if ch.isalnum())


def markdown_table(rows: List[Dict], max_rows: int = 15) -> str:
    """Tabla Markdown con columnas solicitadas (flex match)."""
    if not rows:
        return "_Sin datos_"
    # map flexible (ignorando espacios/case)
    amap = {}
    for k in rows[0].keys():
        ck = _canon(k)
        if ck not in amap:
            amap[ck] = k
    cols = [amap.get(_canon(c)) for c in SUMMARY_COLS if _canon(c) in amap]
    if not cols:
        cols = list(rows[0].keys())[:6]
    header = "| " + " | ".join(cols) + " |"
    sep = "- " + " - ".join(["----------"] * len(cols)) + " - "
    lines = [header, sep]
    for r in rows[:max_rows]:
        lines.append("| " + " | ".join(str(r.get(c, ""))[:64].replace("\n", " ") for c in cols) + " |")
    if len(rows) > max_rows:
        lines.append(f"\n_… {len(rows) - max_rows} filas más_")
    return "\n".join(lines)


# ========= Resolver destino (roomId) =========

def _resolve_room_id(teams_message=None, attachment_actions=None, activity=None) -> Optional[str]:
    """
    Devuelve el roomId correcto para enviar mensajes:
    - Primero intenta teams_message.roomId (cuando escriben "cdr" a mano).
    - Luego attachment_actions.roomId (cuando hacen Submit en la Card).
    - Fallback: activity['roomId'] o activity['target']['globalId'].
    """
    try:
        if getattr(teams_message, "roomId", None):
            return teams_message.roomId
    except Exception:
        pass
    try:
        if getattr(attachment_actions, "roomId", None):
            return attachment_actions.roomId
    except Exception:
        pass
    if isinstance(activity, dict):
        if activity.get("roomId"):
            return activity["roomId"]
        tgt = activity.get("target") or {}
        if isinstance(tgt, dict) and tgt.get("globalId"):
            return tgt["globalId"]
    return None


# ========= Comando CDR + Card =========

class CdrSubmit(Command):
    """Callback para el botón 'Consultar' de la Adaptive Card (callback_keyword='cdr_submit')."""
    def __init__(self):
        super().__init__(card_callback_keyword="cdr_submit", delete_previous_message=False)
        self.api = WebexAPI(access_token=os.getenv("WEBEX_ACCESS_TOKEN"))

    def execute(self, message, attachment_actions, activity):
        admin_token = os.getenv("WEBEX_ADMIN_TOKEN", "").strip()
        if not admin_token:
            return ("❌ Falta **WEBEX_ADMIN_TOKEN** (token admin con scope "
                    "`spark-admin:calling_cdr_read`). Editá HARDCODED_ADMIN_TOKEN al inicio del archivo.")

        # Inputs de la Card
        inputs = getattr(attachment_actions, "inputs", {}) or {}
        start_s = (inputs.get("start") or "").strip()
        end_s   = (inputs.get("end") or "").strip()
        loc_s   = (inputs.get("locations") or "").strip()
        want_csv  = str(inputs.get("csv", "")).lower() in ("true", "1", "yes", "si", "sí")
        want_xlsx = str(inputs.get("excel", "")).lower() in ("true", "1", "yes", "si", "sí")
        rows_val  = str(inputs.get("rows") or "").strip()
        n_rows = int(rows_val) if rows_val.isdigit() else 20

        # Parseo/validación
        try:
            if not start_s or not end_s:
                return "❌ Debés indicar **Inicio** y **Fin**."
            start_dt = parse_local_or_iso(start_s)
            end_dt   = parse_local_or_iso(end_s)
            validate_feed_window(start_dt, end_dt)
        except Exception as e:
            return f"❌ Rango inválido: {e}"

        locations = [x.strip() for x in loc_s.split(",") if x.strip()] if loc_s else None

        # Descarga
        try:
            rows = list(feed_iter(admin_token, start_dt, end_dt, locations))
        except Exception as e:
            return f"❌ Error al consultar CDR Bot: {e}"

        if not rows:
            return "No se encontraron registros para ese rango."

        header = (f"**CDR Bot** {start_dt.isoformat()} → {end_dt.isoformat()} (UTC)\n"
                  f"Registros: **{len(rows)}**\n")
        table_md = markdown_table(rows, max_rows=n_rows)

        # Destino: usar roomId de attachment_actions (o fallback)
        room_id = _resolve_room_id(None, attachment_actions, activity)
        if not room_id:
            return header + "\n" + table_md + "\n\n⚠️ No pude determinar el roomId para adjuntar archivos."

        # Envío (evita responder dentro del hilo del cardAction)
        if want_csv or want_xlsx:
            with tempfile.TemporaryDirectory() as tmpd:
                csv_path = os.path.join(tmpd, "webex_cdr.csv")
                write_csv_all_fields(csv_path, rows)
                self.api.messages.create(room_id, markdown=header + "\n" + table_md)
                self.api.messages.create(room_id, text=os.path.basename(csv_path), files=[csv_path])
                if want_xlsx:
                    try:
                        xlsx_path = os.path.join(tmpd, "webex_cdr.xlsx")
                        to_excel(csv_path, xlsx_path)
                        self.api.messages.create(room_id, text=os.path.basename(xlsx_path), files=[xlsx_path])
                    except Exception as e:
                        self.api.messages.create(room_id, markdown=f"⚠️ No pude generar XLSX: `{e}`")
            return None
        else:
            self.api.messages.create(room_id, markdown=header + "\n" + table_md)
            return None


class CdrCommand(Command):
    def __init__(self):
        super().__init__(
            command_keyword="cdr",
            help_message=(
                "CDR (~48h), clickea acá y llená el formulario."
            ),
            # 👇 registra el callback como "chained command"
            chained_commands=[CdrSubmit()],
        )
        self.api = WebexAPI(access_token=os.getenv("WEBEX_ACCESS_TOKEN"))

    def _card_object(self) -> AdaptiveCard:
        # Título
        title = TextBlock("CDR Bot", weight=FontWeight.BOLDER, size=FontSize.LARGE)
        # Instrucciones
        help1 = TextBlock(
            "Ingresá **Inicio** y **Fin** (≤48 h). "
            "Fin debe ser ≥ 5 min antes de ahora.",
            wrap=True, color=Colors.DARK, horizontalAlignment=HorizontalAlignment.LEFT
        )
        # Inputs
        in_start = Text(id="start", placeholder="2025-08-26 08:00")
        in_end   = Text(id="end",   placeholder="2025-08-26 10:30")
        in_loc   = Text(id="locations", placeholder="Locación (opcional)")
        in_csv   = Text(id="csv",   placeholder="csv? true/false (opcional)")
        in_xlsx  = Text(id="excel", placeholder="excel? true/false (opcional)")
        in_rows  = Text(id="rows",  placeholder="Filas en tabla resumen: ej. 20")

        submit = Submit(title="Consultar", data={"callback_keyword": "cdr_submit"})

        # Armado de la card
        body = [
            ColumnSet(columns=[Column(items=[title])]),
            ColumnSet(columns=[Column(items=[help1])]),
            ColumnSet(columns=[Column(items=[in_start])]),
            ColumnSet(columns=[Column(items=[in_end])]),
            ColumnSet(columns=[Column(items=[in_loc])]),
            ColumnSet(columns=[Column(items=[in_csv])]),
            ColumnSet(columns=[Column(items=[in_xlsx])]),
            ColumnSet(columns=[Column(items=[in_rows])]),
        ]
        return AdaptiveCard(body=body, actions=[submit])

    def execute(self, message, attachment_actions, activity):
        admin_token = os.getenv("WEBEX_ADMIN_TOKEN", "").strip()
        if not admin_token:
            return ("❌ Falta **WEBEX_ADMIN_TOKEN** (token admin con scope "
                    "`spark-admin:calling_cdr_read`). Editá HARDCODED_ADMIN_TOKEN al inicio del archivo.")

        text = (message or "").strip()

        # Si no hay start/end en el texto → devolvemos AdaptiveCard OBJETO
        has_start = re.search(r"\bstart\s*=", text, flags=re.I) is not None
        has_end   = re.search(r"\bend\s*=", text, flags=re.I) is not None
        if not (has_start and has_end):
            card = self._card_object()
            return response_from_adaptive_card(card)

        # Parseo simple k=v
        params: Dict[str, str] = {}
        for m in re.finditer(r"(\w+)\s*=\s*([^\s]+)", text):
            k, v = m.group(1).lower(), m.group(2)
            params[k] = v
        want_csv  = "csv"   in text.lower()
        want_xlsx = "excel" in text.lower()
        n_rows = int(params.get("rows", "20")) if str(params.get("rows", "")).isdigit() else 20

        # Fechas y locations
        try:
            start_dt = parse_local_or_iso(params["start"])
            end_dt   = parse_local_or_iso(params["end"])
            validate_feed_window(start_dt, end_dt)
        except Exception as e:
            return f"❌ Rango inválido: {e}"

        locations = None
        if "locations" in params:
            locations = [x.strip() for x in params["locations"].split(",") if x.strip()]

        # Descarga
        try:
            rows = list(feed_iter(admin_token, start_dt, end_dt, locations))
        except Exception as e:
            return f"❌ Error al consultar CDR: {e}"

        if not rows:
            return "No se encontraron registros para ese rango."

        # Resumen + adjuntos
        header = (f"**CDR Bot** {start_dt.isoformat()} → {end_dt.isoformat()} (UTC)\n"
                  f"Registros: **{len(rows)}**\n")
        table_md = markdown_table(rows, max_rows=n_rows)

        # Si se pidieron adjuntos, enviarlos manualmente al room
        if want_csv or want_xlsx:
            room_id = _resolve_room_id(teams_message=None, attachment_actions=None, activity=activity)
            if not room_id:
                return header + "\n" + table_md + "\n\n⚠️ No pude determinar el roomId para adjuntar archivos."
            with tempfile.TemporaryDirectory() as tmpd:
                csv_path = os.path.join(tmpd, "webex_cdr.csv")
                write_csv_all_fields(csv_path, rows)
                self.api.messages.create(room_id, markdown=header + "\n" + table_md)
                self.api.messages.create(room_id, text=os.path.basename(csv_path), files=[csv_path])
                if want_xlsx:
                    try:
                        xlsx_path = os.path.join(tmpd, "webex_cdr.xlsx")
                        to_excel(csv_path, xlsx_path)
                        self.api.messages.create(room_id, text=os.path.basename(xlsx_path), files=[xlsx_path])
                    except Exception as e:
                        self.api.messages.create(room_id, markdown=f"⚠️ No pude generar XLSX: `{e}`")
            return None

        # Caso simple: dejar que el framework postee el texto devuelto
        return header + "\n" + table_md


# ========= Bootstrap =========

def main():
    bot_token = os.getenv("WEBEX_ACCESS_TOKEN", "").strip()
    if not bot_token:
        print("Falta WEBEX_ACCESS_TOKEN (token del bot). Editá HARDCODED_BOT_TOKEN.", file=sys.stderr)
        sys.exit(2)

    bot = WebexBot(
        teams_bot_token=bot_token,
        bot_name="CDR Bot",
        include_demo_commands=False,
        # Opcional: restringir
        # approved_domains=['tu-dominio.com'],
        # approved_users=['alguien@tu-dominio.com'],
        # approved_rooms=['<ROOM_ID>'],
    )
    bot.add_command(CdrCommand())   # NO registrar CdrSubmit aquí (ya está encadenado)
    bot.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido.")



