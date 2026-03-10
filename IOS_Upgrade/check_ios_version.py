import csv
import re
from typing import List, Dict
from netmiko import ConnectHandler
import os
from dotenv import load_dotenv

# Carrega .env da raiz (opcional)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
load_dotenv(os.path.join(ROOT_DIR, ".env"))


# Versão padrão da empresa
STANDARD_IOS_VERSION = "16.9.5"

# ==== Util de versão ====


def _normalize_version_str(s: str) -> str:
    if not s:
        return ""
    # captura algo como 16.9.5 ou 17.9.4a (mantém dígitos e pontos)
    m = re.search(r"\d+(?:\.\d+){1,3}", s)
    return m.group(0) if m else ""


def _cmp_versions(a: str, b: str) -> int:
    na = _normalize_version_str(a)
    nb = _normalize_version_str(b)
    if not na or not nb:
        return 0  # desconhecido => neutro
    pa = [int(x) for x in na.split('.')]
    pb = [int(x) for x in nb.split('.')]
    # padroniza tamanhos
    L = max(len(pa), len(pb))
    pa += [0] * (L - len(pa))
    pb += [0] * (L - len(pb))
    return (pa > pb) - (pa < pb)

# Helper: extrai IPv4 válido de strings como "IP Address: 10.1.2.3" ou retorna vazio
_ipv4_only = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
_ipv4_labeled = re.compile(r"(?i)(?:^|\b)ip\s*address\s*:\s*(\d{1,3}(?:\.\d{1,3}){3})\b")

def _extract_ip(val: str) -> str:
    if not val:
        return ""
    s = val.strip()
    m = _ipv4_labeled.search(s)
    if m:
        return m.group(1)
    if _ipv4_only.match(s):
        return s
    return ""

# ==== Leitura de switches.csv (com/sem cabeçalho) ====


def load_switches_from_csv(filename: str) -> List[Dict[str, str]]:
    devices: List[Dict[str, str]] = []
    try:
        with open(filename, 'r', encoding='utf-8-sig', newline='') as fh:
            lines = [ln.strip() for ln in fh if ln.strip()]
    except Exception:
        with open(filename, 'r', encoding='cp1252', newline='') as fh:
            lines = [ln.strip() for ln in fh if ln.strip()]
    if not lines:
        return devices

    # Suporta linha "sep=;"
    delimiter = None
    if lines[0].lower().startswith('sep='):
        delimiter = lines[0].split('=', 1)[1][:1]
        lines = lines[1:]
    if delimiter is None:
        delimiter = ';' if any(';' in ln for ln in lines[:5]) else ','

    reader = csv.reader(lines, delimiter=delimiter)
    rows = list(reader)
    if not rows:
        return devices

    # Se a primeira linha parecer cabeçalho com coluna de IP (sanitiza nomes)
    header_raw = [c.strip() for c in rows[0]]
    sanitize = lambda s: re.sub(r"[^a-z0-9]", "", s.lower())
    header_norm = [sanitize(c) for c in header_raw]
    ip_keys_norm = {"ip", "ipaddress", "address", "managementip", "mgmtip"}
    has_header_ip = any(h in ip_keys_norm for h in header_norm)

    start_idx = 1 if has_header_ip else 0
    hmap_norm = {header_norm[i]: i for i in range(len(header_norm))}

    for r in rows[start_idx:]:
        if not r:
            continue
        ip = ""
        if has_header_ip:
            # prioridade para colunas comuns
            for key in ("ipaddress", "ip", "managementip", "mgmtip", "address"):
                idx = hmap_norm.get(key)
                if idx is not None and idx < len(r) and r[idx].strip():
                    cand = _extract_ip(r[idx].strip())
                    if cand:
                        ip = cand
                        break
        else:
            ip = _extract_ip(r[0].strip()) if r else ""
        if ip:
            devices.append({"ip": ip})
    return devices

# ==== Coleta de informações ====


def get_switch_info(switch: Dict[str, str]) -> Dict[str, str]:
    device = {
        "device_type": "cisco_ios",
        "host": switch["ip"],
        "username": switch["username"],
        "password": switch["password"],
        "secret": switch.get("secret", ""),
    }
    info = {"Hostname": switch["ip"], "IP": switch["ip"], "Model": "", "IOS Version": ""}
    try:
        with ConnectHandler(**device) as net_connect:
            try:
                if device["secret"]:
                    net_connect.enable()
            except Exception:
                pass
            output = net_connect.send_command("show version")
            # Hostname pelo prompt (remove # e > no fim)
            prompt = net_connect.find_prompt()
            info["Hostname"] = re.sub(r"[>#]+$", "", prompt).strip()
            # Modelo: tenta padrão "cisco <MODEL> ... processor with"
            mm = re.search(r"(?i)cisco\s+([A-Za-z0-9_-]+)\s+.*?processor with", output)
            if mm:
                info["Model"] = mm.group(1)
            # Fallback: linha "Model number : <MODEL>"
            if not info["Model"]:
                for line in output.splitlines():
                    if "Model number" in line or "Model Number" in line:
                        info["Model"] = line.split(":")[-1].strip()
                        break
            # Versão: padrão "Version X.Y.Z" em IOS/IOS-XE
            mv = re.search(r"(?i)Version\s+([\w.()\-]+)", output)
            if mv:
                info["IOS Version"] = mv.group(1)
    except Exception as e:
        info["Model"] = "Erro"
        info["IOS Version"] = f"Erro: {e}"
    return info

# ==== Main ====


def main():
    import getpass
    # Usa variáveis de ambiente se presentes; senão pergunta
    env_user = os.getenv("SSH_USERNAME")
    env_pass = os.getenv("SSH_PASSWORD")
    if env_user and env_pass:
        username = env_user
        password = env_pass
    else:
        username = input("Usuário SSH: ")
        password = getpass.getpass("Senha SSH: ")

    # Caminhos relativos ao diretório deste script, com override via .env se definido
    switches_csv_env = os.getenv("IOS_SWITCHES_CSV")
    output_csv_env = os.getenv("IOS_OUTPUT_CSV")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    switches_csv_path = switches_csv_env or os.path.join(script_dir, "switches.csv")
    output_csv_path = output_csv_env or os.path.join(script_dir, "switches_result.csv")

    switches = load_switches_from_csv(switches_csv_path)
    results: List[Dict[str, str]] = []
    print("Hostname,IP,Model,IOS Version")
    for sw in switches:
        sw["username"] = username
        sw["password"] = password
        info = get_switch_info(sw)
        print(f'{info["Hostname"]},{info["IP"]},{info["Model"]},{info["IOS Version"]}')
        results.append(info)

    # Salva resultados
    with open(output_csv_path, "w", newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Hostname", "IP", "Model", "IOS Version"])
        for row in results:
            writer.writerow([row.get("Hostname", ""), row.get("IP", ""), row.get("Model", ""), row.get("IOS Version", "")])
    print("\nRelatório salvo em switches_result.csv")

if __name__ == "__main__":
    main()
