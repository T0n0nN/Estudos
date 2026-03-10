import argparse
import csv
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


_TIME_RE = re.compile(r"^(\d{1,2}:)?\d{1,2}:\d{2}(?:\.\d{1,3})?$|^\d+(?:\.\d+)?$")


@dataclass(frozen=True)
class Segment:
    seg_id: str
    start: str
    end: str
    title: str


def _slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9\-_. ]+", "", text)
    text = re.sub(r"\s+", "-", text)
    return text[:80] if text else "clip"


def _validate_time(value: str, field: str) -> str:
    value = value.strip()
    if not _TIME_RE.match(value):
        raise ValueError(
            f"Tempo inválido em '{field}': '{value}'. Use segundos (ex: 75.5) ou HH:MM:SS[.ms] (ex: 00:01:15.500)."
        )
    return value


def read_segments(csv_path: Path) -> list[Segment]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"id", "start", "end", "title"}
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise ValueError(
                f"CSV deve conter colunas: {', '.join(sorted(required))}. Encontrado: {reader.fieldnames}"
            )

        segments: list[Segment] = []
        for row in reader:
            seg_id = (row.get("id") or "").strip()
            start = _validate_time(row.get("start") or "", "start")
            end = _validate_time(row.get("end") or "", "end")
            title = (row.get("title") or "").strip()
            if not seg_id:
                raise ValueError("Campo 'id' vazio em uma linha do CSV")
            if not title:
                title = seg_id
            segments.append(Segment(seg_id=seg_id, start=start, end=end, title=title))

    if not segments:
        raise ValueError("Nenhum segmento encontrado no CSV")
    return segments


def ensure_ffmpeg_available() -> None:
    try:
        subprocess.run(["ffmpeg", "-version"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as exc:
        raise RuntimeError(
            "ffmpeg não encontrado. Instale e garanta que 'ffmpeg' está no PATH. "
            "No Windows, uma forma simples é instalar via winget: winget install Gyan.FFmpeg"
        ) from exc


def build_ffmpeg_cmd(
    *,
    input_path: Path,
    output_path: Path,
    start: str,
    end: str,
    vertical: bool,
    mode: str,
    accurate: bool,
) -> list[str]:
    if vertical and mode == "copy":
        raise ValueError("O modo 'copy' não suporta --vertical (filtros exigem re-encode). Use --mode encode.")

    # Posição do -ss/-to:
    # - Antes do -i: mais rápido, mas pode começar em keyframe (menos preciso).
    # - Depois do -i: mais preciso, mas mais lento.
    if accurate:
        cmd: list[str] = [
            "ffmpeg",
            "-hide_banner",
            "-y",
            "-i",
            str(input_path),
            "-ss",
            start,
            "-to",
            end,
        ]
    else:
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-y",
            "-ss",
            start,
            "-to",
            end,
            "-i",
            str(input_path),
        ]

    if vertical:
        # Center-crop to 9:16 at 1080x1920.
        vf = "scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920"
        cmd += ["-vf", vf]

    if mode == "copy":
        cmd += [
            "-c",
            "copy",
            "-avoid_negative_ts",
            "make_zero",
            "-movflags",
            "+faststart",
            str(output_path),
        ]
        return cmd

    cmd += [
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "20",
        "-c:a",
        "aac",
        "-b:a",
        "160k",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    return cmd


def run_cmd(cmd: list[str]) -> None:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stdout)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Gera vários cortes de um vídeo a partir de um CSV de timestamps (start/end)."
    )
    parser.add_argument("--input", required=True, help="Caminho do vídeo local (mp4/mkv/etc)")
    parser.add_argument("--segments", required=True, help="CSV com colunas: id,start,end,title")
    parser.add_argument("--outdir", default="out", help="Pasta de saída")
    parser.add_argument(
        "--vertical",
        action="store_true",
        help="Gera em 9:16 (1080x1920) com crop central (bom para Reels/Shorts/TikTok)",
    )
    parser.add_argument(
        "--mode",
        choices=["encode", "copy"],
        default="encode",
        help="encode: re-encode (corte mais consistente). copy: muito rápido, sem re-encode (pode começar em keyframe).",
    )
    parser.add_argument(
        "--accurate",
        action="store_true",
        help="Corte mais preciso (mais lento). Coloca -ss/-to após o -i.",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    input_path = Path(args.input).expanduser().resolve()
    segments_path = Path(args.segments).expanduser().resolve()
    outdir = Path(args.outdir).expanduser().resolve()

    if not input_path.exists():
        print(f"Arquivo de vídeo não encontrado: {input_path}", file=sys.stderr)
        return 2
    if not segments_path.exists():
        print(f"CSV não encontrado: {segments_path}", file=sys.stderr)
        return 2

    ensure_ffmpeg_available()
    segments = read_segments(segments_path)

    outdir.mkdir(parents=True, exist_ok=True)

    ok = 0
    for seg in segments:
        name = f"{seg.seg_id}-{_slugify(seg.title)}"
        suffix = "-9x16" if args.vertical else ""
        output_path = outdir / f"{name}{suffix}.mp4"

        cmd = build_ffmpeg_cmd(
            input_path=input_path,
            output_path=output_path,
            start=seg.start,
            end=seg.end,
            vertical=args.vertical,
            mode=args.mode,
            accurate=args.accurate,
        )
        print(f"[+] Gerando {output_path.name} ({seg.start} → {seg.end})")
        try:
            run_cmd(cmd)
            ok += 1
        except Exception as exc:
            print(f"[!] Falhou em {seg.seg_id}: {exc}", file=sys.stderr)

    print(f"\nConcluído: {ok}/{len(segments)} cortes gerados em: {outdir}")
    return 0 if ok == len(segments) else 1


if __name__ == "__main__":
    raise SystemExit(main())
