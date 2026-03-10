import os
import re
import pdfplumber
import imaplib
import email
from email.header import decode_header
from dotenv import load_dotenv
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# ===============================================
# CONFIGURAÇÕES
# ===============================================
load_dotenv()

TARGET_SSIDS = {"mercury", "goweb", "gomobile", "goguest"}

SERVICENOW_URL = os.getenv("SERVICENOW_URL")
SERVICENOW_USER = os.getenv("SERVICENOW_USER")
SERVICENOW_PASS = os.getenv("SERVICENOW_PASS")

EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", 587))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO").split(",")

# ===============================================
# FUNÇÕES DE EMAIL
# ===============================================
def enviar_email(assunto, corpo, anexos=None):
    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(EMAIL_TO)
    msg["Subject"] = assunto

    msg.attach(MIMEText(corpo, "plain"))

    if anexos:
        for anexo in anexos:
            with open(anexo, "rb") as f:
                part = MIMEApplication(f.read(), Name=os.path.basename(anexo))
            part["Content-Disposition"] = f'attachment; filename="{os.path.basename(anexo)}"'
            msg.attach(part)

    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, EMAIL_TO, msg.as_string())

# ===============================================
# SCAN DE PDFs (EXTRAÇÃO SSID + MAC)
# ===============================================
def clean_ssid(ssid):
    if not ssid:
        return None
    return ssid.strip().lower()

def analisar_ssids_em_pdfs(pasta_pdfs):
    """
    Analisa PDFs e retorna dicionário com SSIDs e MACs encontrados.
    {
      "Rogue_AP_-_mx-tultitlan_20250825.pdf": {
         "mercury": ["14:84:73:fe:2b:ac", "70:70:8b:91:03:13"],
         "goweb": ["70:70:8b:26:4d:41"]
      }
    }
    """
    resultados = {}

    for pdf_file in os.listdir(pasta_pdfs):
        if not pdf_file.lower().endswith(".pdf"):
            continue

        caminho_pdf = os.path.join(pasta_pdfs, pdf_file)
        ssid_macs = {}

        try:
            with pdfplumber.open(caminho_pdf) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        for row in table:
                            if not row or len(row) < 7:
                                continue

                            mac_address = row[1]   # Coluna Rogue MAC Address
                            ssid = row[6]          # Coluna SSID

                            if ssid:
                                normalized = clean_ssid(ssid)

                                if normalized in TARGET_SSIDS and ssid.strip().lower() == normalized:
                                    ssid_macs.setdefault(normalized, []).append(mac_address)

        except Exception as e:
            print(f"⚠️ Erro ao processar {pdf_file}: {e}")

        if ssid_macs:
            resultados[pdf_file] = ssid_macs

    return resultados

# ===============================================
# MONTAGEM DO ALERTA
# ===============================================
def montar_alerta(resultados):
    corpo = "🚨 Alerta: AP Rogue Detectado 🚨\n\n"

    for arquivo, ssids in resultados.items():
        # extrair localidade do nome do arquivo (ex: Rogue_AP_-_mx-tultitlan_20250825.pdf)
        match = re.search(r"Rogue.*?-(.*?)(?:_\d+)?\.pdf", arquivo, re.IGNORECASE)
        localidade = match.group(1) if match else "Desconhecida"

        corpo += f"Localidade: {localidade}\n\n"
        for ssid, macs in ssids.items():
            corpo += f"SSID: {ssid}\nMACs:\n"
            for mac in macs:
                corpo += f" - {mac}\n"
            corpo += "\n"

    return corpo

# ===============================================
# MAIN
# ===============================================
if __name__ == "__main__":
    pasta_pdfs = r"C:\Users\za68397\Downloads\AP Rogue"

    resultados = analisar_ssids_em_pdfs(pasta_pdfs)

    if resultados:
        corpo_email = montar_alerta(resultados)
        anexos = [os.path.join(pasta_pdfs, f) for f in resultados.keys()]
        enviar_email("Alerta: AP Rogue Detectado", corpo_email, anexos=anexos)
        print("✅ Alerta enviado com sucesso!")
    else:
        print("⚠️ Nenhum SSID suspeito encontrado.")
