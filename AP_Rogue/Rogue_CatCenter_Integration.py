import os
import win32com.client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import csv
import re
import requests

# Carrega credenciais de Outlook e variáveis gerais (.env)
base_dir = os.path.dirname(__file__)
load_dotenv(os.path.join(base_dir, 'Outlook.env'))
load_dotenv(os.path.join(base_dir, '.env'))

EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')

FOLDER = 'Prime Infrastructure'
SENDER = 'catcenter@goodyear.com'
SUBJECTS = [
    'Rogues-mx-santafe - Notification',
    'Rogues-cr-heredia - Notification',
    'Rogues-ar-buenosaires - Notification',
    'Rogues-mx-tultitlan - Notification',
    'Rogues-pe-callao - Notification',
    'Rogues-mx-elsalto - Notification',
    'Rogues-co-cali - Notification',
    'Rogues-cl-maipu - Notification',
    'Rogues-br-saopaulo - Notification',
    'Rogues-br-americana - Notification',
    'Rogues-mx-sanluispotosi - Notification',
]
CSV_PREFIXES = [
    'Roguesmxsantafe_',
    'Roguescrheredia_',
    'Roguesarbuenosaires_',
    'Roguesmxtultitlan_',
    'Roguespecallao_',
    'Roguesmxelsalto_',
    'Roguescocali_',
    'Roguesclmaipu_',
    'Roguesbrsaopaulo_',
    'Roguesbramericana_',
    'Roguesmxsanluispotosi_',
]
DOWNLOAD_DIR = r'C:\Users\za68397\Downloads\AP Rogue'

# Configuração ServiceNow (lida do .env)
SN_INSTANCE_URL = (os.getenv('SN_INSTANCE_URL') or '').rstrip('/')
SN_USERNAME = os.getenv('SN_USERNAME')
SN_PASSWORD = os.getenv('SN_PASSWORD')

# Forma preferencial: query completa para localizar a Catalog Task
# Exemplo: number=SCTASK0001234^active=true
SN_TASK_QUERY = os.getenv('SN_TASK_QUERY')

# Alternativa: montar a query a partir de grupo e descrição
# Exemplo de valores: SN_TASK_GROUP="LATAM Network" (nome do grupo)
#                     SN_TASK_DESCRIPTION="Daily Rogue AP Check"
SN_TASK_GROUP = os.getenv('SN_TASK_GROUP')
SN_TASK_DESCRIPTION = os.getenv('SN_TASK_DESCRIPTION')

# Mensagem padrão para fechamento automático quando não há SSIDs proibidos
# (pode ser sobrescrita via SN_CLOSE_COMMENTS no .env)
SN_CLOSE_COMMENTS = os.getenv(
    'SN_CLOSE_COMMENTS',
    (
        'Diagnostico: Verificar se alguma rede foi encontrada com os nomes de redes '
        'conhecidos pela Goodyear (Mercury, SuperCushion, GoWeb, GoMobile e GoGuest)\n\n'
        'Resolução: Task feita com sucesso.\n\n'
        'Procedimentos/testes realizados: N/A\n\n'
        'Equipe envolvida na resolução: Telecom\n\n'
        'Usuário ciente do encerramento ? (x)Sim ( )não'
    ),
)

# Exceções temporárias de APs (rogues "aceitos" que não devem gerar alerta)
EXCLUDED_APS = {
    'mx-santafehq01-ap',
    'mx-tultitlandistgarantias-ap',
    'mx-tultitlandistdevoluciones-ap',
}

def download_csv_attachments_outlook():
    outlook = win32com.client.Dispatch('Outlook.Application').GetNamespace('MAPI')
    inbox = outlook.Folders.Item(1).Folders[FOLDER]
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
    # Ordena os e-mails por data de recebimento (do mais recente ao mais antigo)
    inbox.Items.Sort('[ReceivedTime]', True)
    # Calcula o último dia útil anterior
    today = datetime.now()
    weekday = today.weekday()  # 0=segunda, 4=sexta
    if weekday == 0:  # Segunda-feira
        last_business_day = today - timedelta(days=3)
    else:
        last_business_day = today - timedelta(days=1)
    for message in inbox.Items:
        if message.Class != 43:  # OlMailItem
            continue
        if message.SenderEmailAddress.lower() != SENDER.lower():
            continue
        if message.Subject not in SUBJECTS:
            continue
        # Filtra por data de recebimento (último dia útil)
        if not hasattr(message, 'ReceivedTime'):
            continue
        received = message.ReceivedTime
        if received.date() != last_business_day.date():
            continue
        for att in message.Attachments:
            filename = att.FileName
            filepath = os.path.join(DOWNLOAD_DIR, filename)
            if os.path.exists(filepath):
                print(f'Ignorado (já existe): {filepath}')
                continue
            if filename.endswith('.csv') and any(filename.startswith(prefix) for prefix in CSV_PREFIXES):
                att.SaveAsFile(filepath)
                print(f'Baixado: {filepath}')
    # Após baixar/ignorar, varre os arquivos .csv do último dia útil para SSIDs exatos
    ssids = ['mercury', 'goweb', 'gomobile', 'goguest']
    report_lines = []
    csv_files_today = []
    for filename in os.listdir(DOWNLOAD_DIR):
        if filename.endswith('.csv'):
            filepath = os.path.join(DOWNLOAD_DIR, filename)
            # Garante que só analisamos/anexamos arquivos do último dia útil
            try:
                file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
            except OSError:
                continue
            if file_mtime.date() != last_business_day.date():
                continue
            csv_files_today.append(filepath)
            with open(filepath, encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)
                rows = list(reader)
                # Ignora as primeiras 8 linhas (até o cabeçalho)
                if len(rows) < 9:
                    continue
                header = [h.strip().lower() for h in rows[8]]
                def find_col(cols, name):
                    for i, col in enumerate(cols):
                        if name in col:
                            return i
                    return -1
                idx_mac = find_col(header, 'mac address')
                idx_ap = find_col(header, 'detecting ap name')
                idx_ssid = find_col(header, 'ssid')
                if idx_mac == -1 or idx_ap == -1 or idx_ssid == -1:
                    continue
                found = set()
                ssid_details = []
                for row in rows[9:]:
                    if len(row) <= max(idx_mac, idx_ap, idx_ssid):
                        continue
                    cell_ssid = row[idx_ssid].strip()
                    ap_name = row[idx_ap].strip()
                    # Ignora rogues vindos de APs em EXCLUDED_APS (sites com 2 WLCs)
                    if ap_name.strip().lower() in EXCLUDED_APS:
                        continue
                    for ssid in ssids:
                        if re.search(rf'(?i)^({ssid})$', cell_ssid):
                            found.add(ssid)
                            ssid_details.append({
                                'ssid': cell_ssid,
                                'mac': row[idx_mac].strip(),
                                'ap': ap_name,
                            })
                if found:
                    header_line = f'{filename}:'
                    report_lines.append(header_line)
                    for detail in ssid_details:
                        line = f"  SSID: {detail['ssid']} | MAC: {detail['mac']} | AP: {detail['ap']}"
                        report_lines.append(line)

    # Envia email se algum SSID não autorizado foi encontrado
    if report_lines:
        body = '\n'.join(report_lines)
        send_rogues_email(body)
    else:
        print('Nenhum SSID não autorizado encontrado.')
        # Quando não há SSIDs proibidos, tenta atualizar o ServiceNow
        if csv_files_today:
            try:
                close_servicenow_task_with_attachments(csv_files_today)
            except Exception as e:
                print(f'Falha ao atualizar ServiceNow: {e}')


def send_rogues_email(body_text: str) -> None:
    """Envia por email o relatório de SSIDs encontrados.

    Usa o perfil padrão do Outlook. O destinatário pode ser definido na
    variável de ambiente ROGUE_ALERT_TO; caso contrário, usa o EMAIL.
    """
    to_address = os.getenv('ROGUE_ALERT_TO') or EMAIL
    try:
        outlook = win32com.client.Dispatch('Outlook.Application')
        mail = outlook.CreateItem(0)  # OlMailItem
        mail.To = to_address
        mail.Subject = 'Rogues Wireless - SSIDs detectados'
        mail.Body = body_text
        mail.Send()
        print(f'Email de alerta enviado para {to_address}')
    except Exception as e:
        print(f'Falha ao enviar email de alerta: {e}')


def _build_sn_session() -> requests.Session:
    """Cria uma sessão HTTP autenticada para ServiceNow.

    Retorna uma sessão configurada ou levanta ValueError se faltar configuração.
    """
    if not SN_INSTANCE_URL or not SN_USERNAME or not SN_PASSWORD:
        raise ValueError('Variáveis SN_INSTANCE_URL, SN_USERNAME ou SN_PASSWORD não configuradas.')
    session = requests.Session()
    session.auth = (SN_USERNAME, SN_PASSWORD)
    session.headers.update({'Accept': 'application/json'})
    return session


def _find_catalog_task(session: requests.Session):
    """Localiza a Catalog Task (sc_task) que deve ser encerrada.

    Usa SN_TASK_QUERY, se fornecida. Caso contrário, monta a query com
    SN_TASK_GROUP e SN_TASK_DESCRIPTION. Retorna dict com
    (task_sys_id, task_number, ritm_sys_id) ou None se não encontrar.
    """
    if not SN_INSTANCE_URL:
        return None

    url = f"{SN_INSTANCE_URL}/api/now/table/sc_task"

    if SN_TASK_QUERY:
        query = SN_TASK_QUERY
    else:
        parts = []
        if SN_TASK_GROUP:
            # Usa dot-walk para nome do grupo de atribuição
            parts.append(f"assignment_group.name={SN_TASK_GROUP}")
        if SN_TASK_DESCRIPTION:
            parts.append(f"short_description={SN_TASK_DESCRIPTION}")
        # Garante que só pega tasks ativas
        parts.append('active=true')
        query = '^'.join(parts)

    params = {
        'sysparm_fields': 'number,sys_id,short_description,state,active,request_item',
        'sysparm_limit': '1',
        'sysparm_query': query,
    }

    resp = session.get(url, params=params, timeout=30)
    if not resp.ok:
        print(f'Falha ao buscar Catalog Task em ServiceNow: {resp.status_code} {resp.text}')
        return None

    data = resp.json().get('result') or []
    if not data:
        print('Nenhuma Catalog Task encontrada em ServiceNow com a query informada.')
        return None

    task = data[0]
    task_sys_id = task.get('sys_id')
    task_number = task.get('number')
    ritm = task.get('request_item') or {}
    # request_item pode vir como sys_id simples ou como objeto
    if isinstance(ritm, dict):
        ritm_sys_id = ritm.get('value') or ritm.get('sys_id')
    else:
        ritm_sys_id = ritm

    if not task_sys_id or not ritm_sys_id:
        print('Catalog Task encontrada, mas sem sys_id ou request_item válidos.')
        return None

    return {
        'task_sys_id': task_sys_id,
        'task_number': task_number,
        'ritm_sys_id': ritm_sys_id,
    }


def _upload_attachments(session: requests.Session, task_sys_id: str, files: list[str]) -> None:
    """Faz upload dos arquivos CSV como anexos da Catalog Task (sc_task)."""
    if not files:
        return
    url = f"{SN_INSTANCE_URL}/api/now/attachment/upload"
    for path in files:
        if not os.path.isfile(path):
            continue
        filename = os.path.basename(path)
        with open(path, 'rb') as fh:
            files_payload = {'uploadFile': (filename, fh, 'text/csv')}
            data = {
                'table_sys_id': task_sys_id,
                'table_name': 'sc_task',
            }
            resp = session.post(url, files=files_payload, data=data, timeout=60)
            if not resp.ok:
                print(f'Falha ao anexar {filename} à SCTASK {task_sys_id}: {resp.status_code} {resp.text}')
            else:
                print(f'Anexo enviado para ServiceNow: {filename}')


def _close_catalog_task(session: requests.Session, task_sys_id: str, comments: str) -> None:
    """Atualiza o estado da Catalog Task para Closed Complete (3)."""
    url = f"{SN_INSTANCE_URL}/api/now/table/sc_task/{task_sys_id}"
    payload = {
        'state': '3',  # Closed Complete
        'comments': comments,
        'work_notes': comments,
    }
    headers = {'Content-Type': 'application/json'}
    resp = session.patch(url, json=payload, headers=headers, timeout=30)
    if not resp.ok:
        print(f'Falha ao encerrar Catalog Task {task_sys_id}: {resp.status_code} {resp.text}')
    else:
        print(f'Catalog Task {task_sys_id} encerrada com sucesso em ServiceNow.')


def close_servicenow_task_with_attachments(csv_files: list[str]) -> None:
    """Fluxo completo: localizar SCTASK, anexar CSVs e fechar a task.

    Somente é chamado quando **não** há SSIDs proibidos nos relatórios.
    """
    # Se a configuração básica não estiver presente, apenas registra e sai
    if not SN_INSTANCE_URL or not SN_USERNAME or not SN_PASSWORD:
        print('ServiceNow não configurado (SN_INSTANCE_URL/SN_USERNAME/SN_PASSWORD ausentes); nenhuma ação tomada.')
        return

    session = _build_sn_session()
    print(f'Conectando ao ServiceNow em {SN_INSTANCE_URL} com o usuário {SN_USERNAME!r}.')
    task_info = _find_catalog_task(session)
    if not task_info:
        return

    task_sys_id = task_info['task_sys_id']
    task_number = task_info['task_number']
    ritm_sys_id = task_info['ritm_sys_id']

    print(f'Catalog Task localizada em ServiceNow: {task_number} (sys_id={task_sys_id}, RITM={ritm_sys_id})')
    _upload_attachments(session, task_sys_id, csv_files)

    # Monta comentário (inclui a lista de arquivos anexados em nova linha)
    files_str = ', '.join(os.path.basename(f) for f in csv_files)
    comments = f"{SN_CLOSE_COMMENTS}\n\nArquivos anexados: {files_str}"
    _close_catalog_task(session, task_sys_id, comments)

if __name__ == '__main__':
    download_csv_attachments_outlook()
