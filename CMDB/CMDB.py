import os
import csv
import re
import time
import io
from typing import Dict, Any, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from netmiko import ConnectHandler

# Caminhos
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SWITCHES_CSV = os.path.join(SCRIPT_DIR, "switches.csv")
EOX_CATALOG_CSV = os.path.join(SCRIPT_DIR, "eox_catalog.csv")  # opcional
OUTPUT_XLSX = os.path.join(SCRIPT_DIR, "network_eox.xlsx")

# Carrega variáveis de ambiente (Network.env tem prioridade para SSH)
ENV_FILES = [
    (".env", False),        # base
    ("Cisco.env", False),   # Cisco API
    ("Network.env", True),  # credenciais SSH (override=True)
]
for env_name, override in ENV_FILES:
    env_path = os.path.join(SCRIPT_DIR, env_name)
    if os.path.exists(env_path):
        load_dotenv(env_path, override=override)

# Credenciais SSH
NET_USERNAME = os.getenv("NET_USERNAME")
NET_PASSWORD = os.getenv("NET_PASSWORD")
NET_SECRET = os.getenv("NET_SECRET")  # opcional (enable)
if not NET_USERNAME or not NET_PASSWORD:
    raise RuntimeError("Defina NET_USERNAME e NET_PASSWORD em Network.env (ou .env/Cisco.env) na mesma pasta.")

# Credenciais Cisco API (opcional)
CISCO_CLIENT_ID = os.getenv("CISCO_CLIENT_ID")
CISCO_CLIENT_SECRET = os.getenv("CISCO_CLIENT_SECRET")

TIMEOUT = 25
PAUSE_BETWEEN = 0.3

# ==== Cisco EOX API ====

def _get_cisco_token() -> Optional[str]:
    """Obtém token OAuth2 (client_credentials). Retorna None se não configurado ou falhar.
    Tenta primeiro o endpoint novo (id.cisco.com) com Basic Auth; se falhar, tenta o legado (cloudsso).
    """
    if not CISCO_CLIENT_ID or not CISCO_CLIENT_SECRET:
        return None
    # Endpoint novo (recomendado)
    try:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        resp = requests.post(
            "https://id.cisco.com/oauth2/default/v1/token",
            data={"grant_type": "client_credentials"},
            headers=headers,
            auth=requests.auth.HTTPBasicAuth(CISCO_CLIENT_ID, CISCO_CLIENT_SECRET),
            timeout=TIMEOUT,
        )
        if resp.status_code == 200:
            return (resp.json() or {}).get("access_token")
    except Exception:
        pass
    # Fallback para endpoint legado
    try:
        resp = requests.post(
            "https://cloudsso.cisco.com/as/token.oauth2",
            data={
                "grant_type": "client_credentials",
                "client_id": CISCO_CLIENT_ID,
                "client_secret": CISCO_CLIENT_SECRET,
            },
            timeout=TIMEOUT,
        )
        if resp.status_code == 200:
            return (resp.json() or {}).get("access_token")
    except Exception:
        pass
    return None

def _query_cisco_eox(pid: str, token: Optional[str]):
    """Consulta EOX por PID via apix.cisco.com (JSON). Retorna (eol, eos) ou (None, None).
    Em caso de falha (403/596/timeout), tenta fallback em api.cisco.com. Qualquer erro => (None, None).
    """
    if not token or not pid:
        return None, None

    def _do_query(host: str):
        url = f"https://{host}/supporttools/eox/rest/5/EOXByProductID/1/{pid}?format=JSON"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        r = requests.get(url, headers=headers, timeout=TIMEOUT)
        # 403 => sem permissão/assinatura ainda; 596 pode vir do gateway
        if r.status_code == 403:
            return None
        # Alguns gateways retornam 596 (Mashery). Trate como falha para tentar outro host/fallback
        if r.status_code != 200:
            return None
        data = r.json() if r.headers.get("Content-Type", "").lower().startswith("application/json") else r.json()
        items = data.get("EOXRecord") or data.get("EOXRecords") or []
        if isinstance(items, dict):
            items = [items]
        if not items:
            return (None, None)
        rec = items[0] or {}
        eol = ((rec.get("EndOfSaleDate") or {}).get("value")) if isinstance(rec.get("EndOfSaleDate"), dict) else rec.get("EndOfSaleDate")
        eos = ((rec.get("LastDateOfSupport") or {}).get("value")) if isinstance(rec.get("LastDateOfSupport"), dict) else rec.get("LastDateOfSupport")
        if not eos:
            eos = ((rec.get("LastDayOfSupport") or {}).get("value")) if isinstance(rec.get("LastDayOfSupport"), dict) else rec.get("LastDayOfSupport")
        return (eol, eos)

    # Tenta apix primeiro
    try:
        res = _do_query("apix.cisco.com")
        if isinstance(res, tuple):
            return res
    except Exception:
        pass

    # Fallback: tenta host api.cisco.com
    try:
        res = _do_query("api.cisco.com")
        if isinstance(res, tuple):
            return res
    except Exception:
        pass

    return None, None

# ==== Catálogo local EoX ====

def load_local_eox(path: str) -> Dict[str, Dict[str, str]]:
    """Carrega catálogo local (CSV: pid,end_of_life_date,end_of_support_date)."""
    eox: Dict[str, Dict[str, str]] = {}
    if not os.path.exists(path):
        return eox
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            pid = (row.get("pid") or "").strip().upper()
            if not pid:
                continue
            eox[pid] = {
                "eol": (row.get("end_of_life_date") or "").strip(),
                "eos": (row.get("end_of_support_date") or "").strip(),
            }
    return eox

# ==== Coleta no switch ====

def collect_from_switch(ip: str, username: str, password: str, device_type: str = "cisco_ios") -> Dict[str, Any]:
    """Coleta hostname, PID (modelo) e serial via SSH (Cisco IOS/IOS-XE)."""
    info: Dict[str, Any] = {"ip": ip, "hostname": None, "pid": None, "serial": None}
    conn = None
    try:
        conn = ConnectHandler(
            device_type=device_type,
            host=ip,
            username=username,
            password=password,
            fast_cli=True,
        )
        if NET_SECRET:
            try:
                conn.enable()
            except Exception:
                pass
        # Hostname
        out_hn = conn.send_command("show running-config | include ^hostname", expect_string=r"#")
        m = re.search(r"^hostname\s+(\S+)", out_hn, re.MULTILINE)
        if m:
            info["hostname"] = m.group(1)

        # Inventário (PID/Serial)
        out_inv = conn.send_command("show inventory", expect_string=r"#")
        pid_sn = re.search(r"PID:\s*([A-Za-z0-9._-]+).*?SN:\s*([A-Za-z0-9._-]+)", out_inv, re.DOTALL)
        if pid_sn:
            info["pid"] = pid_sn.group(1)
            info["serial"] = pid_sn.group(2)

        # Fallbacks via show version
        if not info["serial"] or not info["pid"]:
            out_ver = conn.send_command("show version", expect_string=r"#")
            if not info["hostname"]:
                mhn = re.search(r"(?i)^hostname\s+(\S+)", out_ver, re.MULTILINE)
                if mhn:
                    info["hostname"] = mhn.group(1)
            if not info["serial"]:
                mser = re.search(r"(?i)(System serial number|Processor board ID)\s*:?\s*(\S+)", out_ver)
                if mser:
                    info["serial"] = mser.group(2)
            if not info["pid"]:
                mpid = re.search(r"(?i)Model number\s*:\s*(\S+)", out_ver)
                if mpid:
                    info["pid"] = mpid.group(1)
    except Exception as e:
        info["error"] = str(e)
    finally:
        try:
            if conn:
                conn.disconnect()
        except Exception:
            pass
    return info

# ==== Leitura robusta do CSV de switches ====

def read_switches(path: str) -> List[Dict[str, str]]:
    """Lê switches.csv aceitando delimitador ';' ou ',' e cabeçalhos alternativos.
       Campos aceitos:
         - IP: ip, address, management_ip, mgmt_ip
         - Location: location, site, local, localidade
         - device_type: device_type, type (default cisco_ios)
         Também suporta CSV sem cabeçalho no formato:
         - ip
         - ip,location
         - ip,location,device_type
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    def _try_read(encoding: str) -> List[Dict[str, str]]:
        with open(path, "r", encoding=encoding, newline="") as fh:
            lines = fh.readlines()
        if not lines:
            return []

        # Detecta linha "sep=;" do Excel
        delimiter = None
        if lines[0].lower().startswith("sep="):
            sep_line = lines[0].strip()
            delimiter = sep_line.split("=", 1)[1][:1] if "=" in sep_line else ";"
            lines = lines[1:]  # remove linha sep=
        # Sniffer para delimitador se ainda não definido
        if delimiter is None:
            sample = "".join(lines[:5])
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=";, \t|")
                delimiter = dialect.delimiter
            except Exception:
                delimiter = ","

        reader = csv.DictReader(io.StringIO("".join(lines)), delimiter=delimiter)
        # Normaliza cabeçalhos
        norm_rows: List[Dict[str, str]] = []
        if not reader.fieldnames:
            return []

        keymap = {k: (k or "").strip().lower().replace(" ", "_") for k in reader.fieldnames}
        ip_keys = {"ip", "address", "management_ip", "mgmt_ip"}
        loc_keys = {"location", "site", "local", "localidade"}
        type_keys = {"device_type", "type"}

        for row in reader:
            # Normaliza chaves da linha
            nrow = {keymap.get(k, k): (v or "").strip() for k, v in row.items()}

            # Resolve IP
            ip_val = ""
            for cand in ip_keys:
                if cand in nrow and nrow[cand]:
                    ip_val = nrow[cand]
                    break
            if not ip_val:
                continue  # sem IP

            # Resolve Location
            loc_val = ""
            for cand in loc_keys:
                if cand in nrow and nrow[cand]:
                    loc_val = nrow[cand]
                    break
            if not loc_val:
                loc_val = "UNKNOWN"

            # Resolve device_type
            dev_type = "cisco_ios"
            for cand in type_keys:
                if cand in nrow and nrow[cand]:
                    dev_type = nrow[cand]
                    break

            norm_rows.append({
                "ip": ip_val,
                "location": loc_val,
                "device_type": dev_type,
            })
        return norm_rows

    def _headerless_fallback(encoding: str) -> List[Dict[str, str]]:
        # Lê CSV sem cabeçalho. Suporta linhas: ip | ip,location | ip,location,device_type
        with open(path, "r", encoding=encoding, newline="") as fh:
            lines = fh.readlines()
        if not lines:
            return []
        # Detecta linha sep=
        delimiter = None
        start = 0
        if lines[0].lower().startswith("sep="):
            sep_line = lines[0].strip()
            delimiter = sep_line.split("=", 1)[1][:1] if "=" in sep_line else ";"
            start = 1
        if delimiter is None:
            sample = "".join(lines[start:start+5])
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                delimiter = dialect.delimiter
            except Exception:
                delimiter = ","
        rows: List[Dict[str, str]] = []
        reader = csv.reader(io.StringIO("".join(lines[start:])), delimiter=delimiter)
        for raw in reader:
            if not raw:
                continue
            # Remove vazios e espaços
            cells = [c.strip() for c in raw if c is not None]
            if not cells or not cells[0]:
                continue
            ip = cells[0]
            location = cells[1] if len(cells) > 1 and cells[1] else "UNKNOWN"
            device_type = cells[2] if len(cells) > 2 and cells[2] else "cisco_ios"
            rows.append({"ip": ip, "location": location, "device_type": device_type})
        return rows

    # Tenta utf-8-sig (remove BOM), depois cp1252
    devices = _try_read("utf-8-sig")
    if not devices:
        devices = _try_read("cp1252")

    # Fallback: arquivo sem cabeçalho (apenas IPs, ou IP+Location[,device_type])
    if not devices:
        devices = _headerless_fallback("utf-8-sig") or _headerless_fallback("cp1252")

    print(f"Carregados {len(devices)} dispositivos de {path}.")
    if len(devices) == 0:
        print("Dica: verifique cabeçalhos (ip, location) e delimitador (',' ou ';').")
        print("Você também pode usar formato simples sem cabeçalho: 'ip' ou 'ip,location[,device_type]'.")
    return devices

# ==== Util ====

def _clean_sheet_name(name: str) -> str:
    bad = set(':\\/*?[]')
    s = ''.join(ch for ch in (name or 'UNKNOWN') if ch not in bad)
    return (s or 'UNKNOWN')[:31]

# ==== Main ====

def main():
    devices = read_switches(SWITCHES_CSV)
    if not devices:
        print("Sem dados para exportar.")
        return

    eox_local = load_local_eox(EOX_CATALOG_CSV)
    token = _get_cisco_token()  # pode ser None

    rows: List[Dict[str, Any]] = []
    for d in devices:
        info = collect_from_switch(d["ip"], NET_USERNAME, NET_PASSWORD, d["device_type"])
        pid_upper = (info.get("pid") or "").upper()

        # Opção C (híbrido): tenta Cisco EOX API primeiro; se falhar/não encontrar, usa catálogo local
        eol = eos = src = ""
        if pid_upper:
            eol_api, eos_api = _query_cisco_eox(pid_upper, token)
            if eol_api or eos_api:
                eol, eos, src = eol_api or "", eos_api or "", "cisco_api"
            elif pid_upper in eox_local:
                eol, eos, src = eox_local[pid_upper]["eol"], eox_local[pid_upper]["eos"], "local"

        rows.append({
            "location": d["location"],
            "hostname": info.get("hostname"),
            "ip": info.get("ip"),
            "pid": info.get("pid"),
            "serial": info.get("serial"),
            "end_of_life": eol,
            "end_of_support": eos,
            "eox_source": src,
            "error": info.get("error", ""),
        })
        time.sleep(PAUSE_BETWEEN)

    df = pd.DataFrame(rows)
    if df.empty:
        print("Sem dados para exportar.")
        return

    mode = "a" if os.path.exists(OUTPUT_XLSX) else "w"
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl", mode=mode, if_sheet_exists="replace") as xw:
        for loc, part in df.groupby("location"):
            part = part.sort_values(["hostname", "ip"], na_position="last")
            sheet = _clean_sheet_name(loc)
            # Se já existir, substitui a aba para manter dados atualizados
            if mode == "a":
                try:
                    wb = xw.book
                    if sheet in wb.sheetnames:
                        ws = wb[sheet]
                        wb.remove(ws)
                except Exception:
                    pass
            part.to_excel(xw, sheet_name=sheet, index=False)

    print(f"Relatório gerado: {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
