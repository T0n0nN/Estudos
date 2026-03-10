import os
import csv
import time
import re
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple

from dotenv import load_dotenv

# Dependência externa: Netmiko
# pip install netmiko
try:
    from netmiko import ConnectHandler
except ImportError:
    ConnectHandler = None  # Tratado no main

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, "Network.env")
# Forçar override para garantir que valores do .env sejam aplicados
load_dotenv(dotenv_path=ENV_PATH, override=True)

# Melhora a descoberta do CSV padrão (variações de nome e busca case-insensitive)
def _default_csv() -> str:
    preferred_names = [
        # nomes mais prováveis (corretos)
        "Switches_IOS_Upgrade_Americana.csv",
        "Switches_IOS_upgrade_Americana.csv",
        "Switches_IOS_Upgrade.csv",
        "Switches_IOS_upgrade.csv",
        "switches_IOS_Upgrade.csv",
        "switches_IOS_upgrade.csv",
        # nomes anteriormente informados com erro de digitação
        "Switcehs_IOS_Upgrade.csv",
        "switcehs_IOS_Upgrade.csv",
    ]
    for name in preferred_names:
        p = os.path.join(SCRIPT_DIR, name)
        if os.path.exists(p):
            return p
    # Busca genérica por qualquer CSV no diretório que contenha termos-chave
    try:
        for fname in os.listdir(SCRIPT_DIR):
            fl = fname.lower()
            if fl.endswith('.csv') and ('switch' in fl) and ('ios' in fl) and ('upgrad' in fl):
                return os.path.join(SCRIPT_DIR, fname)
    except Exception:
        pass
    # Fallback final
    return os.path.join(SCRIPT_DIR, "switches.csv")

DEFAULT_CSV = _default_csv()
RESULT_CSV = os.path.join(SCRIPT_DIR, "switches_result.csv")

# Credenciais: aceitar várias chaves possíveis do .env
NET_USER = (
    os.getenv("NET_SSH_USER")
    or os.getenv("NET_SSH_USERNAME")
    or os.getenv("SSH_USERNAME")
    or os.getenv("NET_USERNAME")
    or os.getenv("USERNAME_NET")
    or os.getenv("CISCO_USER")
    or os.getenv("USERNAME")
)
NET_PASS = (
    os.getenv("NET_SSH_PASS")
    or os.getenv("NET_SSH_PASSWORD")
    or os.getenv("SSH_PASSWORD")
    or os.getenv("NET_PASSWORD")
    or os.getenv("PASSWORD_NET")
    or os.getenv("CISCO_PASS")
    or os.getenv("PASSWORD")
)
NET_ENABLE = (
    os.getenv("NET_ENABLE_PASS")
    or os.getenv("NET_ENABLE_PASSWORD")
    or os.getenv("ENABLE_PASSWORD")
    or os.getenv("ENABLE_PASS")
    or os.getenv("ENABLE")
)

MAX_PARALLEL_DEFAULT = 5
COPY_TIMEOUT_SEC = int(os.getenv("IOS_COPY_TIMEOUT", "3600"))  # até 1h por cópia
VERIFY_TIMEOUT_SEC = int(os.getenv("IOS_VERIFY_TIMEOUT", "900"))  # até 15min MD5

@dataclass
class Switch:
    ip: str
    hostname: str
    tftp_ip: str
    image: str
    md5_expected: Optional[str] = None
    model: Optional[str] = None
    site: Optional[str] = None
    notes: Optional[str] = None


def read_switches(csv_path: str, hosts_filter: Optional[List[str]] = None) -> List[Switch]:
    switches: List[Switch] = []
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV não encontrado: {csv_path}")

    encodings_try = ['utf-8', 'utf-8-sig', 'cp1252', 'latin-1']
    last_err: Optional[Exception] = None
    for enc in encodings_try:
        try:
            with open(csv_path, newline='', encoding=enc) as f:
                sample = f.read(4096)
                f.seek(0)
                delimiter = ';' if sample.count(';') > sample.count(',') else ','
                reader = csv.DictReader(f, delimiter=delimiter)
                # Mapear colunas de forma resiliente (case-insensitive e ignorando espaços/underscore/":")
                def norm(s: Optional[str]) -> str:
                    return re.sub(r"[^a-z0-9]", "", (s or "").lower())
                raw_cols = reader.fieldnames or []
                cols_map = {norm(c): c for c in raw_cols}
                def col(*names: str) -> Optional[str]:
                    for n in names:
                        c = cols_map.get(norm(n))
                        if c:
                            return c
                    return None
                for row in reader:
                    ip = row.get(col('ip', 'ip address', 'ip_address', 'ip address:') or 'ip') or ''
                    hostname = row.get(col('hostname', 'host', 'name') or 'hostname') or ip
                    tftp_ip = row.get(col('tftp_server_ip', 'tftp_serve_ip', 'tftp_ip', 'tftp', 'tftp_server') or '') or ''
                    image = row.get(col('image_filename', 'ios', 'image', 'filename', 'image_file') or '') or ''
                    md5_exp = row.get(col('MD5', 'md5', 'md5sum', 'checksum') or '')
                    model = row.get(col('model') or 'model')
                    site = row.get(col('site') or 'site')
                    notes = row.get(col('notes') or 'notes')
                    if not ip or not tftp_ip or not image:
                        continue
                    if hosts_filter and hostname not in hosts_filter and ip not in hosts_filter:
                        continue
                    switches.append(Switch(ip=ip.strip(), hostname=(hostname or '').strip(),
                                           tftp_ip=tftp_ip.strip(), image=image.strip(),
                                           md5_expected=(md5_exp or '').strip() or None,
                                           model=(model or '').strip() or None,
                                           site=(site or '').strip() or None,
                                           notes=(notes or '').strip() or None))
            # Sucesso na leitura com este encoding
            return switches
        except UnicodeDecodeError as e:
            last_err = e
            continue
    # Se falhou em todos encodings
    if last_err:
        raise last_err
    return switches


def resolve_device_type(model_hint: Optional[str]) -> str:
    # Heurística simples; pode ajustar por modelo no futuro
    return 'cisco_ios'


def connect_switch(sw: Switch):
    if not NET_USER or not NET_PASS:
        raise RuntimeError("Credenciais SSH ausentes. Defina NET_SSH_USER e NET_SSH_PASS em Network.env")
    device = {
        'device_type': resolve_device_type(sw.model),
        'host': sw.ip,
        'username': NET_USER,
        'password': NET_PASS,
        'secret': NET_ENABLE or NET_PASS,
        'fast_cli': False,
    }
    conn = ConnectHandler(**device)
    try:
        conn.enable()
    except Exception:
        pass
    conn.send_command("terminal length 0", expect_string=r"#", strip_prompt=False, strip_command=False)
    return conn


def parse_ping_success(output: str) -> bool:
    # IOS: Success rate is 100 percent (5/5)
    m = re.search(r"Success rate is\s+(\d+) percent", output)
    return bool(m and int(m.group(1)) > 0)


def check_tftp_reachability(conn, tftp_ip: str) -> bool:
    out = conn.send_command(f"ping {tftp_ip}", expect_string=r"#", strip_prompt=False, strip_command=False, delay_factor=2)
    return parse_ping_success(out)


def file_exists_in_flash(conn, filename: str) -> bool:
    out = conn.send_command(f"dir flash:{filename}", expect_string=r"#", strip_prompt=False, strip_command=False)
    return filename in out and "No such file" not in out and "%Error" not in out


# Novo helper genérico: verifica existência em qualquer storage (flash, sdflash, etc.)
def file_exists(conn, storage: str, filename: str) -> bool:
    out = conn.send_command(f"dir {storage}:{filename}", expect_string=r"#", strip_prompt=False, strip_command=False)
    return (
        filename in out
        and "No such file" not in out
        and "%Error" not in out
        and "Invalid input" not in out
    )


def verify_md5(conn, filename: str, timeout_sec: int = VERIFY_TIMEOUT_SEC) -> Tuple[bool, Optional[str]]:
    # Executa verify e retorna (ok, hash)
    out = conn.send_command_timing(f"verify /md5 flash:{filename}", strip_prompt=False, strip_command=False)
    if "\n[yes/no]:" in out or ":[y/n]" in out or "[y/n]" in out or "[yes/no]" in out:
        out += conn.send_command_timing("y\n", strip_prompt=False, strip_command=False)
    # Ler até hash aparecer ou timeout
    end = time.time() + timeout_sec
    buf = out
    hash_val: Optional[str] = None
    while time.time() < end:
        m = re.search(r"([A-Fa-f0-9]{32})", buf)
        if m:
            hash_val = m.group(1).lower()
            break
        time.sleep(2)
        try:
            more = conn.read_channel()
            if more:
                buf += more
        except Exception:
            break
    # Considere OK se obtivemos um hash e não há mensagens claras de erro
    error_words = re.compile(r"(not\s+verified|no\s+such\s+file|%error|failed)", re.I)
    ok = (hash_val is not None) and (error_words.search(buf) is None)
    return ok, hash_val


def verify_md5_storage(conn, storage: str, filename: str, timeout_sec: int = VERIFY_TIMEOUT_SEC) -> Tuple[bool, Optional[str]]:
    # Igual ao verify_md5, porém permitindo storage customizado (flash, sdflash, bootflash, etc.)
    out = conn.send_command_timing(f"verify /md5 {storage}:{filename}", strip_prompt=False, strip_command=False)
    if "\n[yes/no]:" in out or ":[y/n]" in out or "[y/n]" in out or "[yes/no]" in out:
        out += conn.send_command_timing("y\n", strip_prompt=False, strip_command=False)
    end = time.time() + timeout_sec
    buf = out
    hash_val: Optional[str] = None
    while time.time() < end:
        m = re.search(r"([A-Fa-f0-9]{32})", buf)
        if m:
            hash_val = m.group(1).lower()
            break
        time.sleep(2)
        try:
            more = conn.read_channel()
            if more:
                buf += more
        except Exception:
            break
    error_words = re.compile(r"(not\s+verified|no\s+such\s+file|%error|failed)", re.I)
    ok = (hash_val is not None) and (error_words.search(buf) is None)
    return ok, hash_val


def copy_tftp_to_flash(conn, tftp_ip: str, filename: str, timeout_sec: int = COPY_TIMEOUT_SEC) -> bool:
    cmd = f"copy tftp://{tftp_ip}/{filename} flash:{filename}"
    out = conn.send_command_timing(cmd, strip_prompt=False, strip_command=False)
    # Tratar prompts comuns
    if "Address or name of remote host" in out:
        out += conn.send_command_timing(tftp_ip + "\n", strip_prompt=False, strip_command=False)
    if "Source filename" in out:
        out += conn.send_command_timing(filename + "\n", strip_prompt=False, strip_command=False)
    if "Destination filename" in out:
        out += conn.send_command_timing("\n", strip_prompt=False, strip_command=False)  # aceita default
    if "Overwrite" in out:
        out += conn.send_command_timing("y\n", strip_prompt=False, strip_command=False)

    # Aguardar fim da cópia até timeout
    end = time.time() + timeout_sec
    buf = out
    while time.time() < end:
        if re.search(r"\bbytes copied\b|\bCopy complete\b|#\s*$", buf):
            return True
        time.sleep(3)
        try:
            more = conn.read_channel()
            if more:
                buf += more
        except Exception:
            break
    return False


def stage_image(sw: Switch, dry_run: bool = False) -> Dict:
    res = {
        'ip': sw.ip,
        'hostname': sw.hostname,
        'image': sw.image,
        'tftp': sw.tftp_ip,
        'status': 'failed',
        'message': '',
        'md5_computed': None,
        'md5_expected': sw.md5_expected,
    }
    try:
        conn = connect_switch(sw)
    except Exception as e:
        res['message'] = f"SSH/enable falhou: {e}"
        return res

    try:
        # Primeiro: verifique se a imagem já existe (flash ou sdflash)
        exists_flash = file_exists(conn, 'flash', sw.image)
        exists_sd = file_exists(conn, 'sdflash', sw.image)
        if exists_flash or exists_sd:
            storage_used = 'flash' if exists_flash else 'sdflash'
            ok_md5, md5_val = verify_md5_storage(conn, storage_used, sw.image)
            res['md5_computed'] = md5_val
            if not md5_val:
                res['message'] = f"Imagem presente em {storage_used}, mas verify /md5 não retornou hash"
                return res
            if sw.md5_expected and md5_val != sw.md5_expected.lower():
                res['message'] = f"Imagem presente em {storage_used}, porém MD5 divergente (got {md5_val}, expected {sw.md5_expected.lower()})"
                return res
            if ok_md5:
                res['status'] = 'staged'
                res['message'] = f"Imagem já presente em {storage_used} com MD5 ok; cópia ignorada"
                return res

        if dry_run:
            res['status'] = 'skipped'
            res['message'] = 'Dry-run: pular cópia'
            return res

        # Só valida TFTP se precisar copiar
        if not check_tftp_reachability(conn, sw.tftp_ip):
            res['message'] = f"TFTP {sw.tftp_ip} inacessível via ping"
            return res

        # Cópia TFTP -> flash (padrão)
        ok_copy = copy_tftp_to_flash(conn, sw.tftp_ip, sw.image)
        if not ok_copy:
            res['message'] = 'Falha na cópia via TFTP (timeout ou erro)'
            return res

        # Verificar MD5 após cópia (em flash)
        ok_md5, md5_val = verify_md5(conn, sw.image)
        res['md5_computed'] = md5_val
        if not md5_val:
            res['message'] = 'Falha no verify /md5 (sem hash)'
            return res
        if sw.md5_expected and md5_val != sw.md5_expected.lower():
            res['message'] = f"MD5 divergente (got {md5_val}, expected {sw.md5_expected.lower()})"
            return res

        res['status'] = 'staged'
        res['message'] = 'Cópia ok; MD5 computado'
        return res
    except Exception as e:
        res['message'] = f"Erro: {e}"
        return res
    finally:
        try:
            conn.disconnect()
        except Exception:
            pass


def update_switches_md5(csv_path: str, results: List[Dict]):
    # Atualiza a coluna MD5 e IOS Status no CSV de entrada com base nos resultados
    if not os.path.exists(csv_path):
        return

    encodings_try = ['utf-8', 'utf-8-sig', 'cp1252', 'latin-1']
    last_err: Optional[Exception] = None
    rows: List[Dict] = []
    delimiter = ','
    reader_fieldnames: List[str] = []
    for enc in encodings_try:
        try:
            with open(csv_path, newline='', encoding=enc) as f:
                sample = f.read(4096)
                f.seek(0)
                delimiter = ';' if sample.count(';') > sample.count(',') else ','
                reader = csv.DictReader(f, delimiter=delimiter)
                reader_fieldnames = reader.fieldnames or []
                rows = list(reader)
                chosen_enc = enc  # noqa: F841
            break
        except UnicodeDecodeError as e:
            last_err = e
            continue
    if not rows and last_err:
        return

    fieldnames = list(reader_fieldnames)
    if not fieldnames:
        return

    # Normalizador de nomes
    def norm(s: Optional[str]) -> str:
        return re.sub(r"[^a-z0-9]", "", (s or '').lower())

    # Detectar colunas de IP e hostname de forma robusta
    ip_col = None
    host_col = None
    for c in fieldnames:
        nc = norm(c)
        if ip_col is None and nc in ("ip", "ipaddress"):
            ip_col = c
        if host_col is None and nc in ("hostname", "host", "name"):
            host_col = c
    if ip_col is None:
        # fallback comuns
        ip_col = 'ip' if 'ip' in fieldnames else (fieldnames[0] if fieldnames else 'ip')

    # MD5 column
    if 'MD5' not in fieldnames and 'md5' not in fieldnames:
        fieldnames = list(fieldnames) + ['MD5']
        md5_col = 'MD5'
    else:
        md5_col = 'MD5' if 'MD5' in fieldnames else 'md5'

    # IOS Status column
    ios_status_col_candidates = [c for c in fieldnames if norm(c) in ("iosstatus", "statusios")]
    if ios_status_col_candidates:
        ios_status_col = ios_status_col_candidates[0]
    else:
        ios_status_col = 'IOS Status'
        if ios_status_col not in fieldnames:
            fieldnames = list(fieldnames) + [ios_status_col]

    # Mapear resultados por IP/hostname (normalizados)
    def key_ip_host(rec: Dict) -> Tuple[Optional[str], Optional[str]]:
        kip = norm(rec.get('ip')) if rec.get('ip') else None
        kh = norm(rec.get('hostname')) if rec.get('hostname') else None
        return kip, kh

    by_ip: Dict[str, Dict] = {}
    by_host: Dict[str, Dict] = {}
    for r in results:
        kip, kh = key_ip_host(r)
        if kip:
            by_ip[kip] = r
        if kh:
            by_host[kh] = r

    # Reescrever CSV com atualizações (UTF-8 BOM) preservando delimitador e colunas
    tmp_path = csv_path + '.tmp'
    with open(tmp_path, 'w', newline='', encoding='utf-8-sig') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        for row in rows:
            row_ip = (row.get(ip_col) or '').strip()
            row_host = (row.get(host_col) or '').strip() if host_col else ''
            r = by_ip.get(norm(row_ip)) or (by_host.get(norm(row_host)) if row_host else None)
            if r:
                # Atualiza MD5 quando calculado
                if r.get('md5_computed'):
                    row[md5_col] = r['md5_computed']
                # Atualiza IOS Status
                status = (r.get('status') or '').lower()
                if status == 'staged':
                    row[ios_status_col] = 'OK'
                elif status == 'skipped':
                    # Não alterar em dry-run
                    row[ios_status_col] = row.get(ios_status_col, '')
                else:
                    row[ios_status_col] = 'NOK'
            writer.writerow(row)

    # Substituição atômica com backup
    backup = csv_path + '.bak'
    try:
        if os.path.exists(backup):
            os.remove(backup)
    except Exception:
        pass
    try:
        os.replace(csv_path, backup)
    except Exception:
        pass
    # Tentar substituir com algumas tentativas (arquivo pode estar aberto no Excel/OneDrive)
    attempts = 10
    last_err = None
    for _ in range(attempts):
        try:
            os.replace(tmp_path, csv_path)
            last_err = None
            break
        except PermissionError as e:
            last_err = e
            time.sleep(0.7)
        except Exception as e:
            last_err = e
            time.sleep(0.7)
    if last_err:
        # Se não conseguir, tente escrever por cima (fallback) — ainda pode falhar se bloqueado
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f_out:
                # Reescreve conteúdo do tmp
                with open(tmp_path, 'r', encoding='utf-8-sig') as f_in:
                    data = f_in.read()
                f_out.write(data)
            os.remove(tmp_path)
        except Exception:
            # Último recurso: manter backup e alertar o usuário
            print('Aviso: não foi possível atualizar o CSV. Feche o arquivo no Excel/OneDrive e rode novamente.')
            pass


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Staging de IOS via TFTP (upload + verify MD5)")
    parser.add_argument('--csv', default=DEFAULT_CSV, help='Caminho do CSV (padrão: auto-descoberto no diretório)')
    parser.add_argument('--hosts', default=None, help='Lista de hosts/ip separados por vírgula para filtrar')
    parser.add_argument('--max-parallel', type=int, default=MAX_PARALLEL_DEFAULT, help='Máximo de cópias paralelas')
    parser.add_argument('--dry-run', action='store_true', help='Somente validações, não copia')
    args = parser.parse_args()

    if ConnectHandler is None:
        print("❌ Dependência ausente: instale com 'pip install netmiko'")
        return

    # Mostrar qual CSV será usado
    print(f"Usando CSV: {os.path.abspath(args.csv)}")

    hosts_filter = [h.strip() for h in args.hosts.split(',')] if args.hosts else None

    try:
        switches = read_switches(args.csv, hosts_filter)
    except Exception as e:
        print(f"❌ Erro lendo CSV: {e}")
        return

    if not switches:
        print("Nenhum switch para processar.")
        return

    print(f"▶️ Iniciando staging para {len(switches)} switch(es); paralelo: {args.max_parallel}")

    results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=args.max_parallel) as ex:
        futs = {ex.submit(stage_image, sw, args.dry_run): sw for sw in switches}
        for fut in as_completed(futs):
            sw = futs[fut]
            try:
                r = fut.result()
            except Exception as e:
                r = {'ip': sw.ip, 'hostname': sw.hostname, 'image': sw.image, 'tftp': sw.tftp_ip,
                     'status': 'failed', 'message': f'Exceção: {e}', 'md5_expected': sw.md5_expected, 'md5_computed': None}
            results.append(r)
            status = r.get('status')
            msg = r.get('message')
            md5c = r.get('md5_computed')
            print(f"[{status}] {sw.hostname or sw.ip} - {msg}{' | md5=' + md5c if md5c else ''}")

    # Não gerar planilha separada; atualizar o próprio CSV de entrada
    update_switches_md5(args.csv, results)
    print(f"✅ Finalizado. CSV atualizado: {os.path.abspath(args.csv)}")


if __name__ == '__main__':
    main()
