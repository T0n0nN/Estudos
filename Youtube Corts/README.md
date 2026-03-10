# Cortes (workflow rápido)

Este mini-kit gera vários cortes de um vídeo local usando um CSV com timestamps.

## Pré-requisitos

- Ter permissão para usar o conteúdo (ou ser seu).
- `ffmpeg` instalado e no PATH.

### Instalar ffmpeg no Windows (sem winget)

Se o comando `winget` não existe no seu PC, use uma destas opções:

Opção A (manual, funciona em qualquer Windows):

1) Baixe um build do ffmpeg para Windows (ZIP).
2) Extraia para uma pasta, por exemplo: `C:\ffmpeg\`
3) Garanta que exista `C:\ffmpeg\bin\ffmpeg.exe`
4) Adicione `C:\ffmpeg\bin` no PATH do Windows:
   - Pesquisar: "Editar as variáveis de ambiente do sistema" → Variáveis de Ambiente → `Path` → Novo → cole `C:\ffmpeg\bin`
5) Feche e reabra o PowerShell e teste:
   - `ffmpeg -version`

Opção B (instalador de pacotes):

- Chocolatey (se você já usa):
  - `choco install ffmpeg`

- Scoop (se você já usa):
  - `scoop install ffmpeg`

Observação: para ter `winget`, normalmente é preciso ter o "App Installer" atualizado (Microsoft Store) e um Windows 10/11 compatível.

## 1) Criar a lista de cortes

Edite o arquivo `segments.example.csv` e salve como `segments.csv`.

Formato:

- `id`: identificador (001, 002...)
- `start`: início (segundos ou HH:MM:SS[.ms])
- `end`: fim (segundos ou HH:MM:SS[.ms])
- `title`: texto curto para o nome do arquivo

## 2) Gerar os cortes

Na pasta `Youtube Corts/`:

### Cortes longos (recomendado começar)

Para cortes de 3–15 minutos (por assunto), o melhor é exportar rápido e só re-encode quando precisar.

- Export rápido (sem re-encode):
  - `python batch_cuts.py --input "C:\\caminho\\video.mp4" --segments segments.csv --outdir out --mode copy`

Se o início do corte ficar alguns frames antes/depois (por causa de keyframe), use corte mais preciso:

- Corte mais preciso (mais lento):
  - `python batch_cuts.py --input "C:\\caminho\\video.mp4" --segments segments.csv --outdir out --mode encode --accurate`

### Shorts/Reels/TikTok (opcional)

- Vertical 9:16 (re-encode obrigatório):
  - `python batch_cuts.py --input "C:\\caminho\\video.mp4" --segments segments.csv --outdir out --vertical --mode encode`

- Vertical 9:16 (Shorts/Reels/TikTok):
  - `python batch_cuts.py --input "C:\\caminho\\video.mp4" --segments segments.csv --outdir out --vertical`

## Dica de eficiência

- Faça 5–10 cortes longos (3–10 min) por tema.
- Use títulos simples e objetivos (assunto + convidado + benefício).
- Depois que tiver ritmo, aí sim vale testar shorts.
