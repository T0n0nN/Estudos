import os
import csv
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
from urllib.parse import quote

from dotenv import load_dotenv
# Dependência externa: Netmiko
try:
    from netmiko import ConnectHandler
except ImportError:
    ConnectHandler = None

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, "Network.env")
load_dotenv(dotenv_path=ENV_PATH, override=True)

IOS_DEBUG = os.getenv("IOS_DEBUG", "0").lower() in ("1", "true", "yes", "y")

def dbg(msg: str):
    if IOS_DEBUG:
        try:
            print(f"DEBUG: {msg}")
        except Exception:
            pass

# Configuração específica do Chile
CHILE_TFTP_DEFAULT = os.getenv("CHILE_TFTP_IP", "10.107.209.176")
CHILE_TFTP_DIR = os.getenv("CHILE_TFTP_DIR", "IOS Upgrade")
MAX_PARALLEL_DEFAULT = 5
COPY_TIMEOUT_SEC = int(os.getenv("IOS_COPY_TIMEOUT", "3600"))
VERIFY_TIMEOUT_SEC = int(os.getenv("IOS_VERIFY_TIMEOUT", "900"))
READ_TIMEOUT_SEC = float(os.getenv("IOS_READ_TIMEOUT", "0"))  # 0 = sem limite absoluto (recomendado para cópia longa)

# Descoberta do CSV padrão para Chile
def _default_csv() -> str:
    preferred_names = [
        "Switches_IOS_Upgrade_Chile.csv",
        "switches_IOS_Upgrade_Chile.csv",
        "Switches_IOS_upgrade_Chile.csv",
        "switches_IOS_upgrade_Chile.csv",
        # variações com hífens e espaços
        "Switches IOS Upgrade Chile.csv",
        "switches ios upgrade chile.csv",
    ]
    for name in preferred_names:
        p = os.path.join(SCRIPT_DIR, name)
        if os.path.exists(p):
            return p
    # Busca genérica por qualquer CSV com termos-chave
    try:
        for fname in os.listdir(SCRIPT_DIR):
            fl = fname.lower()
            if fl.endswith('.csv') and ('switch' in fl) and ('ios' in fl) and ('upgrad' in fl) and ('chile' in fl):
                return os.path.join(SCRIPT_DIR, fname)
    except Exception:
        pass
    return os.path.join(SCRIPT_DIR, "Switches_IOS_Upgrade_Chile.csv")

DEFAULT_CSV = _default_csv()

# Credenciais SSH dos switches
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


def resolve_device_type(model_hint: Optional[str]) -> str:
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
    dbg(f"connected to {sw.ip} (cmd read_timeout={READ_TIMEOUT_SEC}s)")
    return conn

# Wrappers seguros para versões diferentes do Netmiko

def send_cmd_timing_rt(conn, cmd: str, read_timeout: Optional[float] = None, **kwargs) -> str:
    try:
        if read_timeout is not None:
            return conn.send_command_timing(cmd, read_timeout=read_timeout, **kwargs)
        return conn.send_command_timing(cmd, **kwargs)
    except TypeError:
        # Versão do Netmiko não suporta read_timeout no método
        return conn.send_command_timing(cmd, **kwargs)


def send_cmd_rt(conn, cmd: str, read_timeout: Optional[float] = None, **kwargs) -> str:
    try:
        if read_timeout is not None:
            return conn.send_command(cmd, read_timeout=read_timeout, **kwargs)
        return conn.send_command(cmd, **kwargs)
    except TypeError:
        return conn.send_command(cmd, **kwargs)


def parse_ping_success(output: str) -> bool:
    m = re.search(r"Success rate is\s+(\d+) percent", output)
    return bool(m and int(m.group(1)) > 0)


def check_tftp_reachability(conn, tftp_ip: str) -> bool:
    out = conn.send_command(f"ping {tftp_ip}", expect_string=r"#", strip_prompt=False, strip_command=False, delay_factor=2)
    return parse_ping_success(out)


def file_exists(conn, storage: str, filename: str) -> bool:
    # Primeiro tenta dir com arquivo específico
    cmd1 = f"dir {storage}:{filename}"
    out = conn.send_command(cmd1, expect_string=r"#", strip_prompt=False, strip_command=False)
    if (filename in out) and ("No such file" not in out) and ("%Error" not in out) and ("Invalid input" not in out):
        dbg(f"file_exists: found via '{cmd1}'")
        return True
    # Fallback: lista o diretório inteiro e procura pelo nome
    cmd2 = f"dir {storage}:"
    out2 = conn.send_command(cmd2, expect_string=r"#", strip_prompt=False, strip_command=False)
    ok = (filename in out2) and ("No such file" not in out2) and ("%Error" not in out2)
    dbg(f"file_exists: fallback '{cmd2}' -> {'found' if ok else 'not found'}")
    return ok


def verify_md5_storage(conn, storage: str, filename: str, timeout_sec: int = VERIFY_TIMEOUT_SEC) -> Tuple[bool, Optional[str]]:
    # Tenta diferentes sintaxes de verify /md5, pois variam por plataforma/versão
    cmds = [
        f"verify /md5 {storage}:{filename}",
        f"verify /md5 {storage}:/{filename}",
        f"verify /md5 {storage}: {filename}",
    ]
    error_words = re.compile(r"(not\s+verified|no\s+such\s+file|%error|failed|invalid\s+input)", re.I)
    success_words = re.compile(r"(verified|successfully\s+verified|hash\s+matches)", re.I)

    for cmd in cmds:
        dbg(f"verify_md5: running '{cmd}'")
        out = send_cmd_timing_rt(conn, cmd, read_timeout=READ_TIMEOUT_SEC or 0, strip_prompt=False, strip_command=False)
        if "\n[yes/no]:" in out or ":[y/n]" in out or "[y/n]" in out or "[yes/no]" in out:
            out += send_cmd_timing_rt(conn, "y\n", read_timeout=READ_TIMEOUT_SEC or 0, strip_prompt=False, strip_command=False)
        end = time.time() + timeout_sec
        buf = out
        hash_val: Optional[str] = None
        while time.time() < end:
            m = re.search(r"([A-Fa-f0-9]{32})", buf)
            if m:
                hash_val = m.group(1).lower()
                dbg(f"verify_md5: hash found {hash_val}")
                break
            if success_words.search(buf) and not error_words.search(buf):
                # Sucesso textual mesmo sem hash explícito
                tail = buf[-300:].replace('\r', ' ').replace('\n', ' ')
                dbg(f"verify_md5: success words seen without hash. tail='{tail}'")
                return True, None
            time.sleep(2)
            try:
                more = conn.read_channel()
                if more:
                    buf += more
            except Exception:
                break
        if hash_val:
            ok = error_words.search(buf) is None
            return ok, hash_val
        # se esta tentativa falhou, tenta próxima variante
        dbg("verify_md5: no hash, trying next variant")
    return False, None


def copy_tftp_to_flash(conn, tftp_ip: str, filename: str, timeout_sec: int = COPY_TIMEOUT_SEC) -> bool:
    """Copia via TFTP usando prompts interativos para suportar espaços no caminho
    e diferentes raízes do servidor TFTP. Tenta com CHILE_TFTP_DIR e, se falhar,
    tenta novamente sem o diretório (fallback).
    """
    base_dir = CHILE_TFTP_DIR.strip('/\\') if 'CHILE_TFTP_DIR' in globals() else ''

    def attempt_copy(src_path: str) -> bool:
        dbg(f"copy: interactive from {tftp_ip} src='{src_path}' dest='flash:{filename}'")
        out = send_cmd_timing_rt(conn, "copy tftp: flash:", read_timeout=READ_TIMEOUT_SEC or 0, strip_prompt=False, strip_command=False)
        # Janela para tratar prompts iniciais
        prompts_deadline = time.time() + 30
        handled = set()
        buf = out
        while time.time() < prompts_deadline:
            changed = False
            if ("Address or name of remote host" in buf or "address or name of remote host" in buf) and ('remote' not in handled):
                buf += send_cmd_timing_rt(conn, tftp_ip + "\n", read_timeout=READ_TIMEOUT_SEC or 0, strip_prompt=False, strip_command=False)
                handled.add('remote'); changed = True
            if ("Source filename" in buf or "Source file name" in buf) and ('source' not in handled):
                buf += send_cmd_timing_rt(conn, src_path + "\n", read_timeout=READ_TIMEOUT_SEC or 0, strip_prompt=False, strip_command=False)
                handled.add('source'); changed = True
            if ("Destination filename" in buf or "Destination file name" in buf) and ('dest' not in handled):
                # Use o mesmo nome de arquivo como destino
                buf += send_cmd_timing_rt(conn, filename + "\n", read_timeout=READ_TIMEOUT_SEC or 0, strip_prompt=False, strip_command=False)
                handled.add('dest'); changed = True
            if ("Overwrite" in buf or "overwrite" in buf) and ('overwrite' not in handled):
                buf += send_cmd_timing_rt(conn, "y\n", read_timeout=READ_TIMEOUT_SEC or 0, strip_prompt=False, strip_command=False)
                handled.add('overwrite'); changed = True
            if ("[confirm]" in buf or "confirm]" in buf) and ('confirm' not in handled):
                buf += send_cmd_timing_rt(conn, "\n", read_timeout=READ_TIMEOUT_SEC or 0, strip_prompt=False, strip_command=False)
                handled.add('confirm'); changed = True
            if changed:
                continue
            time.sleep(0.8)
            try:
                more = conn.read_channel()
                if more:
                    buf += more
            except Exception:
                break
        # Loop de transferência
        end = time.time() + timeout_sec
        fail_pat = re.compile(r"(%Error|No such file|timed out|Access violation|Not found|Permission denied|File not found|Error code)", re.I)
        success_pat = re.compile(r"(bytes copied|Copy complete|copied in)", re.I)
        while time.time() < end:
            if success_pat.search(buf):
                dbg("copy: success tokens seen")
                return True
            m_fail = fail_pat.search(buf)
            if m_fail:
                tail = buf[-300:].replace('\r', ' ').replace('\n', ' ')
                dbg(f"copy: failure seen -> {m_fail.group(0)} | tail='{tail}'")
                return False
            time.sleep(2)
            try:
                more = conn.read_channel()
                if more:
                    buf += more
            except Exception:
                break
        dbg("copy: timeout waiting for completion")
        return False

    # 1ª tentativa: com CHILE_TFTP_DIR (se houver)
    src_with_dir = f"{base_dir}/{filename}" if base_dir else filename
    ok = attempt_copy(src_with_dir)
    if ok:
        return True
    # 2ª tentativa: sem diretório (fallback), caso a raiz do TFTP já seja o diretório alvo
    if base_dir:
        dbg("copy: retrying without CHILE_TFTP_DIR (fallback)")
        ok2 = attempt_copy(filename)
        if ok2:
            return True
    return False


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

                    # Default TFTP do Chile, se não informado na planilha
                    if not tftp_ip:
                        tftp_ip = CHILE_TFTP_DEFAULT

                    if not ip or not tftp_ip or not image:
                        continue
                    if hosts_filter and hostname not in hosts_filter and ip not in hosts_filter:
                        continue

                    switches.append(Switch(
                        ip=ip.strip(),
                        hostname=(hostname or '').strip(),
                        tftp_ip=tftp_ip.strip(),
                        image=image.strip(),
                        md5_expected=(md5_exp or '').strip() or None,
                        model=(model or '').strip() or None,
                        site=(site or '').strip() or None,
                        notes=(notes or '').strip() or None,
                    ))
            return switches
        except UnicodeDecodeError as e:
            last_err = e
            continue
    if last_err:
        raise last_err
    return switches


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
        need_copy = True
        # Checa existência
        exists_flash = file_exists(conn, 'flash', sw.image)
        exists_sd = file_exists(conn, 'sdflash', sw.image)
        if exists_flash or exists_sd:
            storage_used = 'flash' if exists_flash else 'sdflash'
            ok_md5, md5_val = verify_md5_storage(conn, storage_used, sw.image)
            res['md5_computed'] = md5_val
            # Se verify indicou sucesso (mesmo sem hash), considerar OK
            if ok_md5 and (not sw.md5_expected or (md5_val and md5_val == sw.md5_expected.lower())):
                res['status'] = 'staged'
                extra = '' if md5_val else ' (sem hash)'
                res['message'] = f"Imagem já presente em {storage_used} com MD5 ok{extra}; cópia ignorada"
                return res
            dbg(f"exists but md5 not ok (ok={ok_md5}, md5={md5_val}, expected={sw.md5_expected}) -> will copy")
            need_copy = True
        else:
            need_copy = True

        if dry_run:
            res['status'] = 'skipped'
            res['message'] = 'Dry-run: pular cópia'
            return res

        if need_copy:
            if not check_tftp_reachability(conn, sw.tftp_ip):
                res['message'] = f"TFTP {sw.tftp_ip} inacessível via ping"
                return res
            ok_copy = copy_tftp_to_flash(conn, sw.tftp_ip, sw.image)
            if not ok_copy:
                res['message'] = 'Falha na cópia via TFTP (timeout ou erro)'
                return res

        # Verificar MD5 em flash após cópia (ou revalidação)
        ok_md5, md5_val = verify_md5_storage(conn, 'flash', sw.image)
        res['md5_computed'] = md5_val
        if not ok_md5:
            res['message'] = 'Falha no verify /md5'
            return res
        # Enforce esperado apenas se há hash calculado
        if sw.md5_expected and md5_val and md5_val != sw.md5_expected.lower():
            res['message'] = f"MD5 divergente (got {md5_val}, expected {sw.md5_expected.lower()})"
            return res

        res['status'] = 'staged'
        res['message'] = 'MD5 ok' + ('' if md5_val else ' (sem hash)')
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
            break
        except UnicodeDecodeError as e:
            last_err = e
            continue
    if not rows and last_err:
        return

    fieldnames = list(reader_fieldnames)
    if not fieldnames:
        return

    def norm(s: Optional[str]) -> str:
        return re.sub(r"[^a-z0-9]", "", (s or '').lower())

    ip_col = None
    host_col = None
    for c in fieldnames:
        nc = norm(c)
        if ip_col is None and nc in ("ip", "ipaddress"):
            ip_col = c
        if host_col is None and nc in ("hostname", "host", "name"):
            host_col = c
    if ip_col is None:
        ip_col = 'ip' if 'ip' in fieldnames else (fieldnames[0] if fieldnames else 'ip')

    if 'MD5' not in fieldnames and 'md5' not in fieldnames:
        fieldnames = list(fieldnames) + ['MD5']
        md5_col = 'MD5'
    else:
        md5_col = 'MD5' if 'MD5' in fieldnames else 'md5'

    ios_status_col_candidates = [c for c in fieldnames if norm(c) in ("iosstatus", "statusios")]
    if ios_status_col_candidates:
        ios_status_col = ios_status_col_candidates[0]
    else:
        ios_status_col = 'IOS Status'
        if ios_status_col not in fieldnames:
            fieldnames = list(fieldnames) + [ios_status_col]

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

    tmp_path = csv_path + '.tmp'
    with open(tmp_path, 'w', newline='', encoding='utf-8-sig') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        for row in rows:
            row_ip = (row.get(ip_col) or '').strip()
            row_host = (row.get(host_col) or '').strip() if host_col else ''
            r = by_ip.get(norm(row_ip)) or (by_host.get(norm(row_host)) if row_host else None)
            if r:
                if r.get('md5_computed'):
                    row[md5_col] = r['md5_computed']
                status = (r.get('status') or '').lower()
                if status == 'staged':
                    row[ios_status_col] = 'OK'
                elif status == 'skipped':
                    row[ios_status_col] = row.get(ios_status_col, '')
                else:
                    row[ios_status_col] = 'NOK'
            writer.writerow(row)

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

    attempts = 10
    last_err = None
    for _ in range(attempts):
        try:
            os.replace(tmp_path, csv_path)
            last_err = None
            break
        except Exception as e:
            last_err = e
            time.sleep(0.7)
    if last_err:
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f_out:
                with open(tmp_path, 'r', encoding='utf-8-sig') as f_in:
                    data = f_in.read()
                f_out.write(data)
            os.remove(tmp_path)
        except Exception:
            print('Aviso: não foi possível atualizar o CSV. Feche o arquivo no Excel/OneDrive e rode novamente.')
            pass


def send_alert_email(to_addr: str, subject: str, body: str) -> bool:
    # Tenta via Outlook (COM). Se falhar, tenta SMTP usando variáveis de ambiente.
    try:
        import win32com.client  # type: ignore
        outlook = win32com.client.Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        mail.To = to_addr
        mail.Subject = subject
        mail.Body = body
        mail.Send()
        return True
    except Exception:
        pass

    try:
        import smtplib
        from email.message import EmailMessage
        host = os.getenv('SMTP_HOST')
        port = int(os.getenv('SMTP_PORT', '587'))
        user = os.getenv('SMTP_USER')
        pwd = os.getenv('SMTP_PASS')
        use_tls = os.getenv('SMTP_USE_TLS', 'true').lower() in ('1', 'true', 'yes', 'y')
        if not host:
            return False
        msg = EmailMessage()
        msg['From'] = os.getenv('SMTP_FROM', user or 'noreply@example.com')
        msg['To'] = to_addr
        msg['Subject'] = subject
        msg.set_content(body)
        with smtplib.SMTP(host, port, timeout=30) as s:
            if use_tls:
                s.starttls()
            if user and pwd:
                s.login(user, pwd)
            s.send_message(msg)
        return True
    except Exception:
        return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Staging de IOS (Chile) via TFTP (upload + verify MD5)")
    parser.add_argument('--csv', default=DEFAULT_CSV, help='Caminho do CSV (padrão: Switches_IOS_Upgrade_Chile.csv)')
    parser.add_argument('--hosts', default=None, help='Lista de hosts/ip separados por vírgula para filtrar')
    parser.add_argument('--max-parallel', type=int, default=MAX_PARALLEL_DEFAULT, help='Máximo de cópias paralelas (padrão=5)')
    parser.add_argument('--dry-run', action='store_true', help='Somente validações, não copia')
    args = parser.parse_args()

    if ConnectHandler is None:
        print("❌ Dependência ausente: instale com 'pip install netmiko'")
        return

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

    max_workers = max(1, min(args.max_parallel, 5))  # nunca passar de 5
    print(f"▶️ Iniciando staging (Chile) para {len(switches)} switch(es); paralelo: {max_workers}")

    results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
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
            if status != 'staged':
                # Enviar alerta por e-mail e seguir para o próximo
                subject = f"Falha no staging de IOS - {sw.hostname or sw.ip}"
                body = (
                    f"Ocorreu um problema durante o staging do IOS.\n"
                    f"Switch: {sw.hostname or ''} ({sw.ip})\n"
                    f"Imagem: {sw.image}\n"
                    f"TFTP: {sw.tftp_ip}\n"
                    f"Mensagem: {msg}"
                )
                ok_mail = send_alert_email("rafael_pereiratonon@goodyear.com", subject, body)
                if not ok_mail:
                    print("Aviso: não foi possível enviar o e-mail de alerta. Configure Outlook ou SMTP_*.")

    update_switches_md5(args.csv, results)
    print(f"✅ Finalizado (Chile). CSV atualizado: {os.path.abspath(args.csv)}")


if __name__ == '__main__':
    main()