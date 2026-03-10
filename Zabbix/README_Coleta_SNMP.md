# Coleta_SNMP (somente leitura)

Coleta via SNMP v2c (somente leitura) os OIDs necessários para validar o template `Portchannel_Monitoring.yaml` (trunks e PortChannels).

## Segurança (não causa outage)
- O script **não executa SNMP SET**.
- Ele apenas faz consultas (GET/BULKWALK) — impacto baixo.
- Se quiser ser mais conservador, reduza `--max-repetitions`.

## Importante (ACL)
O host de onde você roda a coleta precisa estar liberado na ACL do SNMP (`access-list 37` no seu caso). Caso contrário, vai dar timeout.

## Pré-requisitos
- Python 3
- Dependências:

```powershell
python -m pip install -r "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\Zabbix\requirements_coleta_snmp.txt"
```

## Como rodar

```powershell
python "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\Zabbix\Coleta_SNMP.py" --target 10.104.136.1 --community gyplant
```

Exemplo mais “leve”:

```powershell
python "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\Zabbix\Coleta_SNMP.py" --target 10.104.136.1 --community gyplant --max-repetitions 10 --timeout 5 --retries 1
```

## Saída
As coletas ficam em `Zabbix\SNMP_Coletas\<IP>_<timestamp>\` e incluem:
- `meta.txt`
- `summary.txt`
- `sysName.txt` / `sysUpTime.txt`
- `dot3adAggPortTable.txt` (LACP)
- `ifName.txt` / `ifOperStatus.txt`
- `vlanTrunkPortDynamicStatus.txt` (trunk)

## OIDs coletados
- `1.3.6.1.2.1.1.5.0` (sysName.0)
- `1.3.6.1.2.1.1.3.0` (sysUpTime.0)
- `1.3.6.1.4.1.9.9.285.1.1.1.1` (CISCO-PORT-CHANNEL-MIB portChannelTable)
- `1.2.840.10006.300.43.1.2.1.1` (IEEE8023-LAG-MIB dot3adAggPortTable)
- `1.3.6.1.2.1.31.1.1.1.1` (IF-MIB ifName)
- `1.3.6.1.2.1.2.2.1.8` (IF-MIB ifOperStatus)
- `1.3.6.1.2.1.31.1.2.1.3` (IF-MIB ifStackStatus)
- `1.3.6.1.4.1.9.9.46.1.6.1.1.14` (CISCO-VTP-MIB vlanTrunkPortDynamicStatus)
