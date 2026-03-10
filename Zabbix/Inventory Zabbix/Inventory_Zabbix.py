#!/usr/bin/env python3
"""Inventory_Zabbix.py

Script independente para ser usado como "External check" no Zabbix.

Objetivo:
- Receber um identificador (serial ou PID) via linha de comando.
- Autenticar na API da Cisco e consultar informações de EoL/EoS.
- Retornar em stdout APENAS o valor desejado (EOL ou EOS) para o item Zabbix.

Uso sugerido (no Zabbix):
- Copiar este script para o diretório de externalscripts do Zabbix Server.
- Criar um item do tipo "External check" com key, por exemplo:
    Inventory_Zabbix.py["{#SERIAL}", "EOL"]
  ou
    Inventory_Zabbix.py["{#SERIAL}", "EOS"]

- O primeiro parâmetro ($1) é o identificador (serial ou PID).
- O segundo parâmetro ($2) define qual campo retornar: EOL ou EOS (padrão: EOL).

Observação:
- Este script é totalmente independente do EOS&EOL.py; toda a lógica necessária
  para token e consulta EoX está aqui de forma simplificada para uso no Zabbix.
"""

import os
import sys
import json
import time
from datetime import datetime
from urllib.parse import quote

import requests


# Carrega variáveis de ambiente a partir de um arquivo Cisco.env localizado
# na mesma pasta deste script (C:\...\Zabbix\Inventory Zabbix\Cisco.env).
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CISCO_ENV_PATH = os.path.join(SCRIPT_DIR, "Cisco.env")
if os.path.exists(CISCO_ENV_PATH):
    try:
        with open(CISCO_ENV_PATH, "r", encoding="utf-8") as _f:
            for _line in _f:
                _line = _line.strip()
                if not _line or _line.startswith("#"):
                    continue
                if "=" not in _line:
                    continue
                _k, _v = _line.split("=", 1)
                _k = _k.strip()
                _v = _v.strip().strip('"').strip("'")
                if _k and _v and _k not in os.environ:
                    os.environ[_k] = _v
    except Exception:
        # Se der erro ao ler Cisco.env, apenas segue sem interromper o script
        pass


# Configuração básica da API Cisco via variáveis de ambiente (já carregadas do Cisco.env)

def _clean_env(v: str | None) -> str | None:
    if v is None:
        return None
    return v.strip().strip('"').strip("'")


CLIENT_ID = _clean_env(
    os.getenv("CISCO_CLIENT_ID")
    or os.getenv("CLIENT_ID")
    or os.getenv("CCO_CLIENT_ID")
)
CLIENT_SECRET = _clean_env(
    os.getenv("CISCO_CLIENT_SECRET")
    or os.getenv("CLIENT_SECRET")
    or os.getenv("CCO_CLIENT_SECRET")
)
SCOPE = os.getenv("CISCO_SCOPE") or os.getenv("SCOPE") or None
DEBUG_OAUTH = (os.getenv("CISCO_DEBUG_OAUTH") or "0").lower() in ("1", "true", "yes")
DEBUG_EOX = (os.getenv("CISCO_DEBUG_EOX") or "0").lower() in ("1", "true", "yes")

TOKEN_URL = "https://cloudsso.cisco.com/as/token.oauth2"
EOX_BASES = [
    "https://apix.cisco.com/supporttools/eox/rest/5",
    "https://api.cisco.com/supporttools/eox/rest/5",
]
TOKEN_URLS = [
    "https://id.cisco.com/oauth2/default/v1/token",
    TOKEN_URL,
]

TIMEOUT = 30
UA = "Goodyear-EoX-Client/1.0 (+requests)"


def _field_value(rec: dict, key: str) -> str:
    v = rec.get(key)
    if isinstance(v, dict):
        v = v.get("value")
    return v if isinstance(v, str) else (str(v) if v is not None else "")


def _parse_date(value: str) -> str:
    if not value:
        return ""
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(value).strftime("%Y-%m-%d")
        except Exception:
            return ""
    s = str(value).strip()
    if s in ("N/A", "NA", "None", "null", ""):
        return ""
    fmts = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%d-%b-%Y",
        "%b %d, %Y",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    try:
        digits = "".join(ch if ch.isdigit() or ch in "-/" else "" for ch in s)
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(digits, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
    except Exception:
        pass
    return ""


def get_token() -> str:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("Cisco API credentials not found in Cisco.env (CLIENT_ID/CLIENT_SECRET).")
    if DEBUG_OAUTH:
        tail = CLIENT_ID[-4:] if isinstance(CLIENT_ID, str) and len(CLIENT_ID) >= 4 else "????"
        print(f"OAuth: iniciando tentativa de token (client_id=***{tail}, scope={SCOPE or '-'} )")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    attempts_info: list[str] = []

    for url in TOKEN_URLS:
        data1 = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
        if SCOPE:
            data1["scope"] = SCOPE
        try:
            resp1 = requests.post(url, data=data1, headers=headers, timeout=TIMEOUT)
            if DEBUG_OAUTH:
                print(f"OAuth: POST {url} (body creds) -> {resp1.status_code}")
            if resp1.status_code == 200:
                return resp1.json().get("access_token")
            attempts_info.append(f"{url} body={resp1.status_code} {resp1.text[:200] if resp1.text else ''}")
        except Exception as ex:
            attempts_info.append(f"{url} body=exception {ex}")
            if DEBUG_OAUTH:
                print(f"OAuth: exceção em {url} (body creds): {ex}")

        data2 = {"grant_type": "client_credentials"}
        if SCOPE:
            data2["scope"] = SCOPE
        try:
            resp2 = requests.post(url, data=data2, headers=headers, auth=(CLIENT_ID, CLIENT_SECRET), timeout=TIMEOUT)
            if DEBUG_OAUTH:
                print(f"OAuth: POST {url} (basic auth) -> {resp2.status_code}")
            if resp2.status_code == 200:
                return resp2.json().get("access_token")
            attempts_info.append(f"{url} basic={resp2.status_code} {resp2.text[:200] if resp2.text else ''}")
        except Exception as ex:
            attempts_info.append(f"{url} basic=exception {ex}")
            if DEBUG_OAUTH:
                print(f"OAuth: exceção em {url} (basic auth): {ex}")

    summary = " | ".join(attempts_info)
    raise requests.HTTPError(f"Falha no OAuth em todos endpoints. Detalhes: {summary}")


def eox_by_serial(session: requests.Session, token: str, serial: str) -> dict:
    ser = quote(serial, safe="")
    for base in EOX_BASES:
        url = f"{base}/EOXBySerialNumber/1/{ser}?format=json"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json", "User-Agent": UA}
        resp = session.get(url, headers=headers, timeout=TIMEOUT)
        if resp.status_code == 404:
            continue
        try:
            resp.raise_for_status()
        except Exception:
            continue
        data = resp.json() or {}
        records = data.get("EOXRecord") or data.get("Records") or []
        if isinstance(records, dict):
            records = [records]
        if not records:
            continue
        rec = records[0]
        last_support = _field_value(rec, "LastDateOfSupport")
        eol_announce = _field_value(rec, "EOXExternalAnnouncementDate") or _field_value(rec, "EndOfLifeAnnouncementDate")
        end_of_sale = _field_value(rec, "EndOfSaleDate")
        eos = _parse_date(last_support)
        eol_pref = _parse_date(eol_announce) or _parse_date(end_of_sale)
        if eos or eol_pref:
            return {"found": True, "EOS": eos, "EOL": eol_pref}
    return {"found": False}


def eox_by_pid(session: requests.Session, token: str, pid: str) -> dict:
    prod = quote(pid, safe="")
    for base in EOX_BASES:
        url = f"{base}/EOXByProductID/1/{prod}?format=json"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json", "User-Agent": UA}
        resp = session.get(url, headers=headers, timeout=TIMEOUT)
        if resp.status_code == 404:
            continue
        try:
            resp.raise_for_status()
        except Exception:
            continue
        data = resp.json() or {}
        records = data.get("EOXRecord") or data.get("Records") or []
        if isinstance(records, dict):
            records = [records]
        if not records:
            continue
        rec = records[0]
        last_support = _field_value(rec, "LastDateOfSupport")
        eol_announce = _field_value(rec, "EOXExternalAnnouncementDate") or _field_value(rec, "EndOfLifeAnnouncementDate")
        end_of_sale = _field_value(rec, "EndOfSaleDate")
        eos = _parse_date(last_support)
        eol_pref = _parse_date(eol_announce) or _parse_date(end_of_sale)
        if eos or eol_pref:
            return {"found": True, "EOS": eos, "EOL": eol_pref}
    return {"found": False}


def main() -> None:
    # Parâmetros vindos do Zabbix:
    # $1 = identificador (serial ou PID)
    # $2 = campo desejado: "EOL" ou "EOS" (default = "EOL")
    if len(sys.argv) < 2:
        print("")
        return

    identifier = (sys.argv[1] or "").strip()
    if not identifier:
        print("")
        return

    field = "EOL"
    if len(sys.argv) >= 3:
        field = (sys.argv[2] or "EOL").strip().upper()
        if field not in ("EOL", "EOS"):
            field = "EOL"

    try:
        # Vamos tentar interpretar o identificador como serial primeiro,
        # e se necessário você pode adaptar para PID.
        token = get_token()
        session = requests.Session()

        info = eox_by_serial(session, token, identifier)
        if not info.get("found"):
            # Tentar por PID como fallback
            info = eox_by_pid(session, token, identifier)

        # Se ainda assim não achou, retorna vazio
        if not info.get("found"):
            print("")
            return

        eol_val = info.get("EOL", "") or ""
        eos_val = info.get("EOS", "") or ""

        # Normalizar formato de data (YYYY-MM-DD) se possível
        # As funções internas já tentam normalizar, então aqui só garantimos string
        if field == "EOS":
            print(str(eos_val))
        else:
            print(str(eol_val))

    except Exception:
        # Em caso de qualquer erro, devolve vazio (evita quebrar o Zabbix)
        print("")


if __name__ == "__main__":
    main()
