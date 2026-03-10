"""Coleta_SNMP.py

Coleta SOMENTE-LEITURA (SNMP GET/BULKWALK) para validar OIDs usados no template de trunk/portchannel.

- SNMP v2c
- Não executa SNMP SET
- Gera arquivos em SNMP_Coletas/<IP>_<timestamp>/

Requer: pysnmp
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as _dt
import os
import sys
from pathlib import Path
from typing import Iterable, List, Tuple


DEFAULT_TARGET = "10.104.136.1"
DEFAULT_COMMUNITY = "gyplant"


def _require_pysnmp() -> None:
    try:
        import pysnmp
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Dependência ausente: pysnmp. Instale com: python -m pip install -r requirements_coleta_snmp.txt"
        ) from exc

    version = getattr(pysnmp, "__version__", "0")
    # pysnmp antigos (ex.: 4.x) quebram em Python 3.13+ (módulo 'imp' removido).
    major = int(str(version).split(".")[0] or 0)
    if major and major < 7:
        raise RuntimeError(
            f"pysnmp desatualizado (versão {version}). "
            "Atualize com: python -m pip install -U pysnmp pyasn1"
        )


async def _snmp_get(
    snmp_engine,
    auth_data,
    transport_target,
    context_data,
    oid: str,
) -> List[str]:
    from pysnmp.hlapi.v3arch import ObjectIdentity, ObjectType, get_cmd  # type: ignore

    error_indication, error_status, error_index, var_binds = await get_cmd(
        snmp_engine,
        auth_data,
        transport_target,
        context_data,
        ObjectType(ObjectIdentity(oid)),
    )

    if error_indication:
        return [f"ERROR: {error_indication}"]
    if error_status:
        return [f"ERROR: {error_status.prettyPrint()} at {error_index}"]

    lines: List[str] = []
    for var_bind in var_binds:
        # var_bind is ObjectType
        name = var_bind[0]
        val = var_bind[1]
        lines.append(f"{name.prettyPrint()} = {val.prettyPrint()}")
    return lines


async def _snmp_bulkwalk(
    snmp_engine,
    auth_data,
    transport_target,
    context_data,
    base_oid: str,
    max_repetitions: int,
) -> List[str]:
    """Walk a subtree using pysnmp 7.x bulk_walk_cmd.

    Stops when returned OID is outside base_oid prefix.
    """

    from pysnmp.hlapi.v3arch import ObjectIdentity, ObjectType, bulk_walk_cmd  # type: ignore

    base_oid_clean = base_oid.strip(".")
    base_tuple = tuple(int(part) for part in base_oid_clean.split(".") if part)
    lines: List[str] = []

    async for (
        error_indication,
        error_status,
        error_index,
        var_binds,
    ) in bulk_walk_cmd(
        snmp_engine,
        auth_data,
        transport_target,
        context_data,
        0,
        max_repetitions,
        ObjectType(ObjectIdentity(base_oid)),
        lexicographicMode=False,
    ):
        if error_indication:
            lines.append(f"ERROR: {error_indication}")
            break
        if error_status:
            lines.append(f"ERROR: {error_status.prettyPrint()} at {error_index}")
            break

        finished = False
        for var_bind in var_binds:
            name = var_bind[0]
            val = var_bind[1]
            oid_tuple = name.asTuple()
            if oid_tuple[: len(base_tuple)] != base_tuple:
                finished = True
                break
            oid_str = ".".join(str(x) for x in oid_tuple)
            lines.append(f"{oid_str} = {val.prettyPrint()}")
        if finished:
            break

    return lines


def _write_lines(path: Path, lines: Iterable[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _count_nonempty_lines(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip())


async def _amain(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="Coleta_SNMP",
        description="Coleta SNMP v2c (somente leitura) para validar trunks e PortChannels (Zabbix 7.x).",
    )
    parser.add_argument("--target", default=DEFAULT_TARGET, help="IP/DNS do switch")
    parser.add_argument("--community", default=DEFAULT_COMMUNITY, help="SNMP community (v2c)")
    parser.add_argument("--timeout", type=int, default=5, help="Timeout em segundos")
    parser.add_argument("--retries", type=int, default=1, help="Número de retries")
    parser.add_argument(
        "--max-repetitions",
        type=int,
        default=50,
        help="Tamanho do BULKWALK (quanto maior, mais rápido; quanto menor, mais leve)",
    )
    parser.add_argument(
        "--out-root",
        default=str(Path(__file__).resolve().parent / "SNMP_Coletas"),
        help="Diretório base de saída",
    )

    args = parser.parse_args(argv)

    _require_pysnmp()

    # pysnmp 7.x HLAPI is asyncio-first; create shared session objects
    from pysnmp.hlapi.v3arch import (  # type: ignore
        CommunityData,
        ContextData,
        SnmpEngine,
        UdpTransportTarget,
    )

    snmp_engine = SnmpEngine()
    auth_data = CommunityData(args.community, mpModel=1)
    transport_target = await UdpTransportTarget.create(
        (args.target, 161), timeout=args.timeout, retries=args.retries
    )
    context_data = ContextData()

    timestamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = Path(args.out_root) / f"{args.target}_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    oids: List[Tuple[str, str, str]] = [
        ("sysName", "1.3.6.1.2.1.1.5.0", "get"),
        ("sysUpTime", "1.3.6.1.2.1.1.3.0", "get"),
        (
            "ciscoPortChannelTable",
            "1.3.6.1.4.1.9.9.285.1.1.1.1",
            "walk",
        ),
        ("dot3adAggPortTable", "1.2.840.10006.300.43.1.2.1.1", "walk"),
        ("ifName", "1.3.6.1.2.1.31.1.1.1.1", "walk"),
        ("ifOperStatus", "1.3.6.1.2.1.2.2.1.8", "walk"),
        ("ifStackStatus", "1.3.6.1.2.1.31.1.2.1.3", "walk"),
        ("vlanTrunkPortDynamicStatus", "1.3.6.1.4.1.9.9.46.1.6.1.1.14", "walk"),
    ]

    meta = [
        f"Target={args.target}",
        f"Community={args.community}",
        f"Timestamp={timestamp}",
        f"TimeoutSeconds={args.timeout}",
        f"Retries={args.retries}",
        f"MaxRepetitions={args.max_repetitions}",
        "Mode=READONLY",
        "Note=No SNMP SET is performed",
    ]
    _write_lines(out_dir / "meta.txt", meta)

    print(f"Coletando SNMP v2c (somente leitura) de {args.target}...")
    print(f"Saída: {out_dir}")

    for name, oid, mode in oids:
        out_file = out_dir / f"{name}.txt"
        print(f"- {name} ({oid})")
        if mode == "get":
            lines = await _snmp_get(snmp_engine, auth_data, transport_target, context_data, oid)
        else:
            lines = await _snmp_bulkwalk(
                snmp_engine,
                auth_data,
                transport_target,
                context_data,
                oid,
                args.max_repetitions,
            )
        _write_lines(out_file, lines)

    summary_lines = [
        f"sysName first line: {(out_dir / 'sysName.txt').read_text(encoding='utf-8', errors='replace').splitlines()[:1]}",
        f"ciscoPortChannelTable lines: {_count_nonempty_lines(out_dir / 'ciscoPortChannelTable.txt')}",
        f"dot3adAggPortTable lines: {_count_nonempty_lines(out_dir / 'dot3adAggPortTable.txt')}",
        f"ifName lines: {_count_nonempty_lines(out_dir / 'ifName.txt')}",
        f"ifOperStatus lines: {_count_nonempty_lines(out_dir / 'ifOperStatus.txt')}",
        f"ifStackStatus lines: {_count_nonempty_lines(out_dir / 'ifStackStatus.txt')}",
        f"vlanTrunkPortDynamicStatus lines: {_count_nonempty_lines(out_dir / 'vlanTrunkPortDynamicStatus.txt')}",
    ]
    _write_lines(out_dir / "summary.txt", summary_lines)

    print("Concluído. Veja:", out_dir / "summary.txt")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_amain(sys.argv[1:])))
