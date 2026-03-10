#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from netmiko import ConnectHandler
import csv
import getpass
import os
import sys
import time
import traceback
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from dotenv import load_dotenv
from typing import Tuple
import threading
import socket

# -------------------------
# Carregar .env (raiz do workspace e pasta atual)
# -------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
load_dotenv(os.path.join(ROOT_DIR, ".env"))
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))

# -------------------------
#          Configuráveis
# -------------------------
BIN_FILENAME    = os.getenv("IOS_BIN_FILENAME", "cat9k_iosxe.17.9.4.bin")
# Ajuste: por padrão, procurar o switches.csv dentro da subpasta 'Americana'
DEFAULT_CSV_PATH = os.path.join(SCRIPT_DIR, "Americana", "switches.csv")
CSV_FILE        = os.getenv("IOS_SWITCHES_CSV", DEFAULT_CSV_PATH)
MAX_WORKERS     = int(os.getenv("IOS_MAX_WORKERS", "3"))
CONNECT_TIMEOUT = int(os.getenv("IOS_CONNECT_TIMEOUT", "10"))   # segundos
RETRY_COUNT     = int(os.getenv("IOS_RETRY_COUNT", "2"))
RETRY_DELAY     = int(os.getenv("IOS_RETRY_DELAY", "5"))        # segundos entre tentativas
# Storage preferido: "auto" tenta detectar (flash:/bootflash:), ou force com "flash:" ou "bootflash:"
STORAGE_PREF    = os.getenv("IOS_STORAGE", "flash:").strip().lower()
# Fail-fast: cancela operações pendentes após primeira falha
FAIL_FAST       = (os.getenv("IOS_FAIL_FAST", "1").strip().lower() in ("1", "true", "yes"))
CANCEL_EVENT    = threading.Event()
# Espera por reload e validação pós-instalação
WAIT_RELOAD     = (os.getenv("IOS_WAIT_RELOAD", "1").strip().lower() in ("1", "true", "yes"))
DOWN_WAIT_SEC   = int(os.getenv("IOS_DOWN_WAIT_SEC", "600"))     # máx. 10 min p/ cair
UP_WAIT_SEC     = int(os.getenv("IOS_UP_WAIT_SEC", "1800"))      # máx. 30 min p/ voltar
POLL_INTERVAL   = int(os.getenv("IOS_POLL_INTERVAL", "10"))      # intervalo entre tentativas
# Por padrão, usa comando em uma única linha (add file ... activate commit). Para dividir em passos, defina IOS_SPLIT_INSTALL=1
SPLIT_INSTALL   = (os.getenv("IOS_SPLIT_INSTALL", "0").strip().lower() in ("1", "true", "yes"))

# -------------------------
#          Internos
# -------------------------
def setup_logger() -> Tuple[logging.Logger, str]:
    """
    Configura um logger com um handler para arquivo (DEBUG)
    e um handler para console (INFO). Retorna o objeto logger
    e o nome do arquivo de log.
    """
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"resultado_instalacao_{ts}.log"

    logger = logging.getLogger("IOSInstaller")
    logger.setLevel(logging.DEBUG)

    # File handler (DEBUG)
    fh = logging.FileHandler(log_filename, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh_fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    fh.setFormatter(fh_fmt)
    logger.addHandler(fh)

    # Console handler (INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch_fmt = logging.Formatter("%(message)s")
    ch.setFormatter(ch_fmt)
    logger.addHandler(ch)

    return logger, log_filename


def read_switches(csv_path: str) -> list:
    """
    Lê o CSV de switches e retorna lista de dicts.
    Gera erro se o arquivo não existir ou estiver vazio.
    Valida colunas necessárias: Nome, IP, BIN
    """
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"Arquivo '{csv_path}' não encontrado.")
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV sem cabeçalho.")
        campos = set(reader.fieldnames)
        faltando = {"Nome", "IP", "BIN"} - campos
        if faltando:
            raise ValueError(f"CSV precisa conter as colunas: Nome, IP, BIN. Encontrado: {', '.join(reader.fieldnames)}")
        switches = list(reader)
        if not switches:
            raise ValueError(f"Nenhum switch encontrado em '{csv_path}'.")
        # Validar BIN preenchido por linha
        linhas_sem_bin = [str(i + 2) for i, r in enumerate(switches) if not (r.get("BIN") or "").strip()]
        if linhas_sem_bin:
            raise ValueError(
                "Coluna BIN obrigatória e vazia nas linhas: " + ", ".join(linhas_sem_bin)
            )
    return switches


def _enable_if_needed(conn, secret: str | None, logger: logging.Logger | None = None):
    try:
        if secret:
            conn.enable()
        # evitar paginação
        try:
            conn.send_command("terminal length 0", expect_string=r"#", delay_factor=2)
        except Exception:
            pass
    except Exception as e:
        if logger:
            logger.debug(f"Falha ao entrar em enable: {e}")


def _detect_storage(conn) -> str:
    """Detecta 'flash:' ou 'bootflash:' no IOS-XE. Retorna um deles (fallback 'flash:')."""
    try:
        out = conn.send_command("dir bootflash:", expect_string=r"#", delay_factor=2)
        if "Directory of" in out or "bytes" in out.lower():
            return "bootflash:"
    except Exception:
        pass
    try:
        out = conn.send_command("dir flash:", expect_string=r"#", delay_factor=2)
        if "Directory of" in out or "bytes" in out.lower():
            return "flash:"
    except Exception:
        pass
    return "flash:"


def _image_exists(conn, storage: str, image_name: str) -> bool:
    try:
        out = conn.send_command(f"dir {storage}{image_name}", expect_string=r"#", delay_factor=3)
        return image_name in out and ("No such file" not in out and "Error" not in out)
    except Exception:
        return False


def _is_install_mode(conn) -> bool:
    """Verifica modo INSTALL com heurística robusta (summary e version)."""
    try:
        cmds = [
            "show install summary | i [Oo]perating|Mode|mode",
            "show install summary",
            "show version | i Mode|INSTALL|BUNDLE",
            "show version",
        ]
        for cmd in cmds:
            try:
                out = conn.send_command(cmd, expect_string=r"#", delay_factor=2)
            except Exception:
                continue
            ol = (out or "").lower()
            # Se mencionar INSTALL e não mencionar BUNDLE, consideramos INSTALL
            if "install" in ol and "bundle" not in ol:
                return True
            # Se mencionar explicitamente BUNDLE sem INSTALL, consideramos não INSTALL
            if "bundle" in ol and "install" not in ol:
                return False
        return False
    except Exception:
        return False


def _has_fatal_error(txt: str) -> Tuple[bool, str]:
    t = (txt or "").lower()
    patterns = [
        "%error", " error:", " error ", " failed", "failure", "not enough space", "insufficient space",
        "cannot proceed", "not in install mode", "verification failed", "hash verification failed",
        "integrity check failed", "install add failed", "activate failed", "commit failed",
        "compatibility check failed", "no such file", "no space", "abort" 
    ]
    for p in patterns:
        if p in t:
            return True, p
    return False, ""


def test_credentials(switch: dict, username: str, password: str, secret: str | None) -> bool:
    """
    Tenta se conectar ao primeiro switch apenas para validar credenciais.
    """
    device = {
        "device_type":  "cisco_xe",
        "host":         switch.get("IP"),
        "username":     username,
        "password":     password,
        "timeout":      CONNECT_TIMEOUT,
        "secret":       secret or "",
    }
    try:
        conn = ConnectHandler(**device)
        _enable_if_needed(conn, secret, None)
        # Comando simples para validar privilégios
        conn.send_command("show clock", expect_string=r"#", delay_factor=1)
        conn.disconnect()
        return True
    except Exception:
        return False


def _extract_version_prefix_from_bin(bin_name: str) -> str | None:
    """Extrai prefixo de versão (ex.: 17.15.03) do nome do BIN, se possível."""
    import re
    m = re.search(r"(\d+\.\d+\.\d+)", bin_name)
    return m.group(1) if m else None


def _tcp_can_connect(host: str, port: int = 22, timeout: int = 3) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def _await_reload_and_reconnect(device: dict, logger: logging.Logger) -> Tuple[bool, object, str]:
    """Aguarda SSH cair e voltar. Retorna (True, conn, msg) em sucesso, ou (False, None, motivo)."""
    host = device.get("host")
    # Espera SSH DOWN
    start = time.time()
    saw_down = False
    while time.time() - start < DOWN_WAIT_SEC:
        if not _tcp_can_connect(host):
            saw_down = True
            break
        time.sleep(POLL_INTERVAL)
    if not saw_down:
        # Pode ter sido muito rápido; segue para tentativa de UP mesmo assim
        logger.debug(f"{host} ➜ SSH não ficou indisponível dentro do tempo; seguindo para reconexão.")
    # Espera SSH UP e reconectar
    start = time.time()
    while time.time() - start < UP_WAIT_SEC:
        if _tcp_can_connect(host):
            try:
                conn = ConnectHandler(**device)
                return True, conn, "Reconectado"
            except Exception:
                pass
        time.sleep(POLL_INTERVAL)
    return False, None, "Não reconectou dentro do tempo limite"


def _committed_version_matches(conn, expected_prefix: str | None) -> Tuple[bool, str]:
    """Confere se há versão 'C' (Committed) que casa com o prefixo esperado (ex.: 17.15.03)."""
    try:
        out = conn.send_command("show install summary", expect_string=r"#", delay_factor=3)
        lines = (out or "").splitlines()
        committed = []
        for ln in lines:
            # Procura linhas tipo: 'IMG   C    17.15.03.0.xxxx'
            if "IMG" in ln and " C " in ln:
                parts = ln.split()
                # última coluna tende a ser a versão
                if parts:
                    committed.append(parts[-1])
        if not committed:
            return False, "Nenhuma versão 'C' encontrada"
        if expected_prefix:
            ok = any(v.startswith(expected_prefix) for v in committed)
            return ok, ", ".join(committed)
        # Se não temos prefixo, consideramos sucesso se há qualquer 'C'
        return True, ", ".join(committed)
    except Exception as e:
        return False, str(e)


def _running_boot_lines(conn) -> list[str]:
    """Retorna linhas de 'boot system' do running-config."""
    try:
        out = conn.send_command("show run | i ^boot system", expect_string=r"#", delay_factor=2)
    except Exception:
        out = ""
    return [ln.strip() for ln in (out or "").splitlines() if ln and ln.strip().startswith("boot system")]


def _ensure_boot_points_to_packages(conn, storage: str, logger: logging.Logger) -> Tuple[bool, str]:
    """
    Garante que o BOOT aponte para packages.conf (INSTALL mode).
    - Verifica 'show boot' e 'show run | i ^boot system'.
    - Se necessário, remove linhas existentes de boot system e configura 'boot system <storage>packages.conf'.
    - Salva config e revalida.
    Retorna (ok, mensagem).
    """
    try:
        sb = conn.send_command("show boot", expect_string=r"#", delay_factor=3)
    except Exception:
        sb = ""
    try:
        sr_lines = _running_boot_lines(conn)
    except Exception:
        sr_lines = []
    text = ((sb or "") + "\n" + "\n".join(sr_lines)).lower()
    if "packages.conf" in text:
        return True, "boot já aponta para packages.conf"

    # Ajustar configuração
    try:
        conn.config_mode()
    except Exception:
        pass

    # Remover boot system existentes
    for ln in list(sr_lines):
        try:
            conn.send_command_timing("no " + ln, delay_factor=3, max_loops=200)
        except Exception:
            pass

    # Adicionar boot system correto
    boot_path = (storage if storage else "bootflash:") + "packages.conf"
    try:
        conn.send_command_timing(f"boot system {boot_path}", delay_factor=3, max_loops=200)
    except Exception:
        pass

    try:
        if conn.check_config_mode():
            conn.exit_config_mode()
    except Exception:
        pass

    # Salvar config
    try:
        w = conn.send_command_timing("write memory", delay_factor=5, max_loops=200)
        wl = (w or "").lower()
        if "[confirm]" in wl:
            conn.send_command_timing("\n", delay_factor=3, max_loops=100)
        elif "[y/n]" in wl:
            conn.send_command_timing("y", delay_factor=3, max_loops=100)
        elif "[yes/no]" in wl:
            conn.send_command_timing("yes", delay_factor=3, max_loops=100)
    except Exception:
        pass

    # Revalidar
    try:
        sb2 = conn.send_command("show boot", expect_string=r"#", delay_factor=3)
    except Exception:
        sb2 = ""
    try:
        sr2 = conn.send_command("show run | i ^boot system", expect_string=r"#", delay_factor=2)
    except Exception:
        sr2 = ""
    ok = ("packages.conf" in (sb2 or "").lower()) or ("packages.conf" in (sr2 or "").lower())
    return ok, ("OK" if ok else "não foi possível confirmar no show boot")


def _run_long_operation(conn, cmd: str, max_seconds: int = 5400, logger: logging.Logger | None = None) -> str:
    """Executa comandos longos (install ...) mantendo o canal aberto,
    respondendo a prompts e coletando saída por até max_seconds.
    """
    out = conn.send_command_timing(cmd, delay_factor=12, max_loops=2000, strip_command=False, strip_prompt=False)
    start = time.time()
    while time.time() - start < max_seconds:
        low = (out or "").lower()
        # Responder prompts comuns
        if "[yes/no]" in low or " yes/no]" in low:
            out += "\n" + conn.send_command_timing("yes", delay_factor=12, max_loops=2000)
            continue
        if "[y/n]" in low or " y/n]" in low or ("proceed" in low and "[y/n]" in low):
            out += "\n" + conn.send_command_timing("y", delay_factor=12, max_loops=2000)
            continue
        if "[confirm]" in low:
            out += "\n" + conn.send_command_timing("\n", delay_factor=12, max_loops=2000)
            continue
        if "save? [yes/no]" in low or "save the configuration" in low:
            out += "\n" + conn.send_command_timing("no", delay_factor=12, max_loops=2000)
            continue
        if "press return to continue" in low or "press enter to continue" in low:
            out += "\n" + conn.send_command_timing("\n", delay_factor=12, max_loops=2000)
            continue

        # Ler mais dados do canal
        try:
            more = conn.read_channel()
        except Exception:
            more = ""
        if more:
            out += more
            time.sleep(2)
            continue

        # Heurísticas de término/sinalização de reload
        if any(k in low for k in [
            "install add operation successful",
            "finished install operation",
            "activating software",
            "switch will be reloaded",
            "system will be reloaded",
            "this operation will reload the system",
            "reload command is being issued",
            "chassis will be rebooted",
            "rebooting",
            "packages will be activated at next reload",
        ]):
            break
        time.sleep(3)
    if logger:
        try:
            logger.debug(f"Saída acumulada de '{cmd}':\n{out}")
        except Exception:
            pass
    return out


def install_ios_on_device(switch: dict, username: str, password: str, secret: str | None, logger: logging.Logger) -> tuple:
    """
    Instala o IOS no switch. Faz RETRY_COUNT tentativas em caso de falha.
    Loga detalhes de debug no arquivo e resumo no console.
    """
    nome = switch.get("Nome", "Unknown")
    ip   = switch.get("IP")
    device = {
        "device_type":  "cisco_xe",
        "host":         ip,
        "username":     username,
        "password":     password,
        "timeout":      CONNECT_TIMEOUT,
        "secret":       secret or "",
        "fast_cli":     False,  # interações mais estáveis/esperas maiores
    }

    # Imagem deve vir do CSV (coluna obrigatória BIN)
    bin_file = (switch.get("BIN") or "").strip()
    expected_prefix = _extract_version_prefix_from_bin(bin_file)

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            if CANCEL_EVENT.is_set():
                return nome, ip, "CANCELADO (fail-fast)"

            if not bin_file:
                msg = "Coluna BIN obrigatória não preenchida para este dispositivo."
                logger.error(f"{nome} ({ip}) ➜ {msg}")
                if FAIL_FAST:
                    CANCEL_EVENT.set()
                return nome, ip, f"ERRO: {msg}"

            conn = ConnectHandler(**device)
            _enable_if_needed(conn, secret, logger)

            # Verificar privilégios
            try:
                priv = conn.send_command("show privilege", expect_string=r"#", delay_factor=2)
                import re
                m = re.search(r"(\d+)", priv or "")
                if m and int(m.group(1)) < 15:
                    try:
                        conn.disconnect()
                    except Exception:
                        pass
                    logger.error(f"{nome} ({ip}) ➜ Privilégio insuficiente (priv {m.group(1)}). Necessário 15 para install/reload.")
                    if FAIL_FAST:
                        CANCEL_EVENT.set()
                    return nome, ip, "ERRO: privilégio < 15"
            except Exception:
                # se falhar, continua, mas pode causar negação de comandos
                pass

            # Verificar modo INSTALL
            if not _is_install_mode(conn):
                try:
                    conn.disconnect()
                except Exception:
                    pass
                msg = "Equipamento não está em modo INSTALL. Abortando para segurança."
                logger.error(f"{nome} ({ip}) ➜ {msg}")
                if FAIL_FAST:
                    CANCEL_EVENT.set()
                return nome, ip, f"ERRO: {msg}"

            # Detectar storage
            storage = STORAGE_PREF if STORAGE_PREF in ("flash:", "bootflash:") else _detect_storage(conn)

            # Garantir boot para packages.conf
            okboot, msgboot = _ensure_boot_points_to_packages(conn, storage, logger)
            if not okboot:
                try:
                    conn.disconnect()
                except Exception:
                    pass
                logger.error(f"{nome} ({ip}) ➜ ERRO ao ajustar boot para packages.conf: {msgboot}")
                if FAIL_FAST:
                    CANCEL_EVENT.set()
                return nome, ip, f"ERRO: boot packages.conf ({msgboot})"
            else:
                logger.debug(f"{nome} ({ip}) ➜ boot para packages.conf verificado/ajustado: {msgboot}")

            # Checar se a imagem existe
            if not _image_exists(conn, storage, bin_file):
                msg = f"Imagem não encontrada em {storage}{bin_file}. Pule o dispositivo ou copie a imagem."
                logger.error(f"{nome} ({ip}) ➜ {msg}")
                try:
                    conn.disconnect()
                except Exception:
                    pass
                if FAIL_FAST:
                    CANCEL_EVENT.set()
                return nome, ip, msg

            if CANCEL_EVENT.is_set():
                try:
                    conn.disconnect()
                except Exception:
                    pass
                return nome, ip, "CANCELADO (fail-fast)"

            # Executar instalação (separada por padrão)
            outputs = []
            if SPLIT_INSTALL:
                # 1) ADD
                add_cmd = f"install add file {storage}{bin_file}"
                out_add = _run_long_operation(conn, add_cmd, max_seconds=7200, logger=logger)
                outputs.append(("add", out_add))
                fatal, marker = _has_fatal_error(out_add)
                if fatal:
                    try:
                        conn.disconnect()
                    except Exception:
                        pass
                    logger.error(f"{nome} ({ip}) ➜ ERRO durante 'install add': {marker}")
                    if FAIL_FAST:
                        CANCEL_EVENT.set()
                    return nome, ip, f"ERRO: {marker}"
                # 2) ACTIVATE
                act_cmd = "install activate"
                out_act = _run_long_operation(conn, act_cmd, max_seconds=7200, logger=logger)
                outputs.append(("activate", out_act))
                fatal, marker = _has_fatal_error(out_act)
                if fatal:
                    try:
                        conn.disconnect()
                    except Exception:
                        pass
                    logger.error(f"{nome} ({ip}) ➜ ERRO durante 'install activate': {marker}")
                    if FAIL_FAST:
                        CANCEL_EVENT.set()
                    return nome, ip, f"ERRO: {marker}"
                # 3) COMMIT (rápido; pode retornar 'No changes to commit')
                try:
                    com_out = conn.send_command_timing("install commit", delay_factor=10, max_loops=500)
                except Exception:
                    com_out = ""
                outputs.append(("commit", com_out))
            else:
                cmd  = f"install add file {storage}{bin_file} activate commit"
                out = _run_long_operation(conn, cmd, max_seconds=7200, logger=logger)
                outputs.append(("combo", out))

            # Consolida saídas para análise
            out_all = "\n\n".join([f"## {step}\n{txt}" for step, txt in outputs])

            # Loop para responder prompts remanescentes (defensivo)
            for _ in range(5):
                low = (out_all or "").lower()
                fatal, marker = _has_fatal_error(out_all)
                if fatal:
                    try:
                        conn.disconnect()
                    except Exception:
                        pass
                    logger.error(f"{nome} ({ip}) ➜ ERRO detectado após install: {marker}")
                    if FAIL_FAST:
                        CANCEL_EVENT.set()
                    return nome, ip, f"ERRO: {marker}"
                if any(x in low for x in ("[yes/no]", " y/n]", "[confirm]", "save the configuration")):
                    if "[yes/no]" in low:
                        out_all += "\n" + conn.send_command_timing("yes", delay_factor=12, max_loops=2000)
                        continue
                    if "[y/n]" in low or " y/n]" in low:
                        out_all += "\n" + conn.send_command_timing("y", delay_factor=12, max_loops=2000)
                        continue
                    if "[confirm]" in low:
                        out_all += "\n" + conn.send_command_timing("\n", delay_factor=12, max_loops=2000)
                        continue
                    if "save the configuration" in low:
                        out_all += "\n" + conn.send_command_timing("no", delay_factor=12, max_loops=2000)
                        continue
                break

            # Logar a saída do install para diagnóstico
            try:
                logger.debug(f"{nome} ({ip}) ➜ saída do install (consolidada):\n{out_all}")
            except Exception:
                pass

            lower = (out_all or "").lower()
            success_markers = [
                "install add operation successful",
                "install operation will continue",
                "finished install operation",
                "activating software",
                "switch will be reloaded",
                "system will be reloaded",
                "this operation will reload the system",
                "reload command is being issued",
                "chassis will be rebooted",
                "rebooting",
            ]
            reload_required_markers = [
                "you must reload the system",
                "reload is required",
                "please reload the system",
                "needs a reload",
                "packages will be activated at next reload",
            ]

            # Se vai recarregar automaticamente
            will_reload = any(m in lower for m in [
                "switch will be reloaded", "system will be reloaded", "this operation will reload the system",
                "reload command is being issued", "chassis will be rebooted", "rebooting"
            ])

            if WAIT_RELOAD:
                reload_sent = False
                # Se não está claro que vai recarregar, decidir próximo passo
                if not will_reload:
                    # Olhar o summary: se a versão alvo já aparece, ativar e recarregar
                    try:
                        summary = conn.send_command("show install summary", expect_string=r"#", delay_factor=3)
                    except Exception:
                        summary = ""
                    sm_low = (summary or "").lower()
                    exp_in_summary = bool(expected_prefix and (expected_prefix.lower() in sm_low))

                    if any(m in lower for m in reload_required_markers) or exp_in_summary:
                        if exp_in_summary:
                            logger.info(f"{nome} ({ip}) ➜ ativando imagem antes do reload (versão vista no summary)")
                            act_out = conn.send_command_timing("install activate", delay_factor=10, max_loops=500)
                            for _ in range(10):
                                lowa = (act_out or "").lower()
                                if "[yes/no]" in lowa or " yes/no]" in lowa:
                                    act_out += "\n" + conn.send_command_timing("yes", delay_factor=10, max_loops=500)
                                    continue
                                if "[y/n]" in lowa or " y/n]" in lowa or ("proceed" in lowa and "[y/n]" in lowa):
                                    act_out += "\n" + conn.send_command_timing("y", delay_factor=10, max_loops=500)
                                    continue
                                if "[confirm]" in lowa:
                                    act_out += "\n" + conn.send_command_timing("\n", delay_factor=10, max_loops=500)
                                    continue
                                break
                            try:
                                logger.debug(f"{nome} ({ip}) ➜ saída do 'install activate' (pré-reload):\n{act_out}")
                            except Exception:
                                pass
                            # Tentar commit (pode responder "No changes to commit", o que é OK)
                            try:
                                com_out = conn.send_command_timing("install commit", delay_factor=10, max_loops=500)
                                logger.debug(f"{nome} ({ip}) ➜ saída do 'install commit' (pré-reload):\n{com_out}")
                            except Exception:
                                pass

                        logger.info(f"{nome} ({ip}) ➜ enviando reload explícito (exp_in_summary={exp_in_summary})")
                        r_out = _send_reload_with_confirms(conn)
                        try:
                            logger.debug(f"{nome} ({ip}) ➜ saída do 'reload':\n{r_out}")
                        except Exception:
                            pass
                        rl = (r_out or "").lower()
                        if any(x in rl for x in [
                            "authorization failed", "command authorization failed", "not authorized",
                            "insufficient privileges", "privilege level", "permission denied", "disabled by aaa"
                        ]):
                            logger.error(f"{nome} ({ip}) ➜ reload negado por AAA/permissões")
                            will_reload = False
                            reload_sent = False
                        else:
                            will_reload = True
                            reload_sent = True
                    else:
                        # Fallback: reforçar em 3 passos e então recarregar
                        logger.info(f"{nome} ({ip}) ➜ reforçando instalação (add ➜ activate ➜ commit)")
                        out2 = conn.send_command_timing(f"install add file {storage}{bin_file}", delay_factor=10, max_loops=400)
                        out3 = conn.send_command_timing("install activate", delay_factor=10, max_loops=500)
                        out4 = conn.send_command_timing("install commit", delay_factor=10, max_loops=500)
                        try:
                            logger.debug(f"{nome} ({ip}) ➜ saída reforço add/activate/commit:\n{out2}\n{out3}\n{out4}")
                        except Exception:
                            pass
                        r_out = _send_reload_with_confirms(conn)
                        try:
                            logger.debug(f"{nome} ({ip}) ➜ saída do 'reload' (após reforço):\n{r_out}")
                        except Exception:
                            pass
                        rl = (r_out or "").lower()
                        if any(x in rl for x in [
                            "authorization failed", "command authorization failed", "not authorized",
                            "insufficient privileges", "privilege level", "permission denied", "disabled by aaa"
                        ]):
                            logger.error(f"{nome} ({ip}) ➜ reload negado por AAA/permissões (após reforço)")
                            will_reload = False
                            reload_sent = False
                        else:
                            will_reload = True
                            reload_sent = True

                if will_reload:
                    # Fechar sessão e aguardar reload/retorno
                    try:
                        conn.disconnect()
                    except Exception:
                        pass
                    logger.info(f"{nome} ({ip}) ➜ aguardando reload e validação...")
                    ok, new_conn, reason = _await_reload_and_reconnect(device, logger)
                    if not ok or not new_conn:
                        if FAIL_FAST:
                            CANCEL_EVENT.set()
                        logger.error(f"{nome} ({ip}) ➜ ERRO pós-reload: {reason}")
                        return nome, ip, f"ERRO: {reason}"
                    # Validar modo e versão
                    _enable_if_needed(new_conn, secret, logger)
                    if not _is_install_mode(new_conn):
                        try:
                            new_conn.disconnect()
                        except Exception:
                            pass
                        if FAIL_FAST:
                            CANCEL_EVENT.set()
                        return nome, ip, "ERRO: pós-reload não está em INSTALL"
                    okv, info = _committed_version_matches(new_conn, expected_prefix)
                    try:
                        new_conn.disconnect()
                    except Exception:
                        pass
                    if okv:
                        logger.info(f"{nome} ({ip}) ➜ SUCESSO (validado): {info}")
                        return nome, ip, "SUCESSO"
                    else:
                        if FAIL_FAST:
                            CANCEL_EVENT.set()
                        logger.error(f"{nome} ({ip}) ➜ ERRO validação versão: {info}")
                        return nome, ip, f"ERRO: validação versão ({info})"
                else:
                    # Nada indica que um reload ocorrerá. Não desconectar à toa e sinalizar.
                    logger.error(f"{nome} ({ip}) ➜ Nenhum indicador de reload após install; verifique permissões/saída do comando.")
                    try:
                        conn.disconnect()
                    except Exception:
                        pass
                    return nome, ip, "ERRO: reload não disparou"

            # Se não aguardamos reload, usar heurística de sucesso imediata
            if any(m in lower for m in success_markers):
                try:
                    conn.disconnect()
                except Exception:
                    pass
                logger.info(f"{nome} ({ip}) ➜ SUCESSO (comando enviado)")
                return nome, ip, "SUCESSO"

            try:
                conn.disconnect()
            except Exception:
                pass
            logger.info(f"{nome} ({ip}) ➜ COMANDO ENVIADO")
            return nome, ip, "COMANDO ENVIADO"

        except Exception as e:
            tb = traceback.format_exc()
            logger.debug(f"[{nome} - tentativa {attempt}] erro: {e}\n{tb}")

            if attempt < RETRY_COUNT:
                time.sleep(RETRY_DELAY)
            else:
                if FAIL_FAST:
                    CANCEL_EVENT.set()
                logger.error(f"{nome} ({ip}) ➜ ERRO: {e}")
                return nome, ip, f"ERRO: {e}"


def _send_reload_with_confirms(conn) -> str:
    """Envia 'reload' e confirma prompts comuns. Retorna o output consolidado."""
    r = conn.send_command_timing("reload", delay_factor=5, max_loops=200)
    rl = (r or '').lower()
    if "[confirm]" in rl:
        rl += "\n" + conn.send_command_timing("\n", delay_factor=5, max_loops=200)
    if "[y/n]" in rl:
        rl += "\n" + conn.send_command_timing("y", delay_factor=5, max_loops=200)
    if "[yes/no]" in rl:
        rl += "\n" + conn.send_command_timing("yes", delay_factor=5, max_loops=200)
    if "save? [yes/no]" in rl or "save the configuration" in rl:
        rl += "\n" + conn.send_command_timing("no", delay_factor=5, max_loops=200)
    return rl


def main():
    # Credenciais via .env (opcional) ou prompt
    username = os.getenv("SSH_USERNAME") or input("👤 Usuário: ")
    password = os.getenv("SSH_PASSWORD") or getpass.getpass("🔒 Senha: ")
    # Não perguntar por enable; usaremos apenas se vier por variável de ambiente
    enable_secret = os.getenv("SSH_ENABLE_SECRET") or None

    # Leitura dos switches
    try:
        switches = read_switches(CSV_FILE)
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)

    # Teste de credenciais
    first = switches[0]
    print(f"\n🔍 Testando credenciais em {first['Nome']} ({first['IP']})...")
    if not test_credentials(first, username, password, enable_secret):
        print("❌ Credenciais inválidas ou privilégios insuficientes. Abortando script.")
        sys.exit(1)
    print("✅ Credenciais válidas. Iniciando instalação...\n")

    # Configuração de logging
    logger, log_filename = setup_logger()
    logger.info("▶️  Início do processo de instalação de IOS")

    sucesso = 0
    falha  = 0

    # Execução paralela
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(install_ios_on_device, sw, username, password, enable_secret, logger): sw
            for sw in switches
        }

        for future in tqdm(as_completed(futures),
                           total=len(futures),
                           desc="🔄 Atualizando switches",
                           ncols=100):
            nome, ip, resultado = future.result()
            tqdm.write(f"{nome} ({ip}) ➜ {resultado}")
            if "SUCESSO" in resultado:
                sucesso += 1
            elif "CANCELADO" in resultado and FAIL_FAST:
                falha += 1
            elif "ERRO" in resultado:
                falha += 1
            # Se fail-fast e houve erro, sinal já foi acionado; apenas informativo

    # Resumo final
    print("\n📊 Instalação concluída!")
    print(f"✅ Sucesso: {sucesso}")
    print(f"❌ Falhas: {falha}")
    print(f"📝 Log salvo em: {log_filename}")


if __name__ == "__main__":
    main()
