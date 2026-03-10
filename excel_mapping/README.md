# AnyLAN Mapping (Excel)

Gera um arquivo Excel no mesmo layout do print (Panel Port, Tether 1/2, colunas A–M, e cabeçalhos dos cabos), para você atualizar os nomes conforme o diagrama.

## Como gerar

No PowerShell:

```powershell
& "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\.venv\Scripts\python.exe" "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\excel_mapping\generate_mapping_excel.py"
```

O arquivo sai em:
- `excel_mapping/AnyLAN_Mapping.xlsx`

## Como preencher

Edite `excel_mapping/mapping_data.json`.

O arquivo suporta múltiplas abas via `sheets`:

```json
{
	"sheets": {
		"Cable 11 and 12": {
			"title_left": "MultiMode AnyLAN Cable 11",
			"title_right": "MultiMode AnyLAN Cable 12",
			"left_letters": ["A", "B", "C", "D", "E", "F"],
			"right_letters": ["G", "H", "J", "K", "L", "M"],
			"tether1": {"A": ["SW1", "SW2"]},
			"tether2": {"A": ["SW3", "SW4"]}
		}
	}
}
```

Em cada tether (`tether1` e `tether2`), cada letra (A..M) recebe **2 valores**, um para cada retângulo/TAP:

Exemplo (Tether 1, letra A):

```json
{
	"tether1": {
		"A": ["SW1 (Tap 1)", "SW2 (Tap 2)"]
	}
}
```

Observação: a numeração de TAPs no Excel já segue a regra:
- Cable 11 (A..F): A=1/2, B=3/4, ..., F=11/12
- Cable 12 (G..M): G=1/2, H=3/4, ..., M=11/12

## Conectores novos (Portas 1..6, sem TAP)

Algumas letras podem usar um layout diferente: **6 portas (1..6)** e **sem TAPs**.

Para ativar isso em uma aba, informe `port6_letters` com as letras que devem usar o layout novo.

Se o conector tiver **apenas 6 portas no total** (e nao 6 por tether), use `port6_total_across_tethers: true`.
Nesse modo, as letras em `port6_letters` usam **3 portas no Tether 1** e **3 portas no Tether 2**,
mantendo o tamanho da tabela. A coluna `PORT` (se habilitada) fica como 1..3 em cima e 4..6 embaixo.

Nessas letras, em vez de 2 valores, você passa **6 valores** (um por porta 1..6) para cada tether.

Exemplo:

```json
{
	"sheets": {
		"Cable 15": {
			"left_letters": ["A", "B", "C", "D", "E", "F"],
			"right_letters": ["G", "H", "J", "K", "L", "M"],
			"port6_letters": ["E", "F", "G", "H", "J", "K", "L", "M"],
			"port6_total_across_tethers": true,
			"port_column_after": "D",
			"port_column_title": "PORT",
			"tether1": {
				"A": ["SW1 (Tap 1)", "SW2 (Tap 2)"],
				"E": ["Port 1", "Port 2", "Port 3", "Port 4", "Port 5", "Port 6"]
			},
			"tether2": {
				"A": ["SW3 (Tap 1)", "SW4 (Tap 2)"],
				"E": ["Port 1", "Port 2", "Port 3", "Port 4", "Port 5", "Port 6"]
			}
		}
	}
}
```

## Atualizar automaticamente

Se quiser que o Excel seja regenerado automaticamente quando o `mapping_data.json` mudar:

```powershell
& "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\.venv\Scripts\python.exe" "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\excel_mapping\generate_mapping_excel.py" --watch
```

Alternativa mais prática (sem depender do PowerShell Execution Policy): use o `excel_mapping/run_watch.bat`.

Duplo clique no `run_watch.bat` (ou rode no terminal):

```powershell
& "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\excel_mapping\run_watch.bat"
```

Para acompanhar apenas uma aba:

```powershell
& "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\excel_mapping\run_watch.bat" "Cable 11 and 12"
```

No modo `--watch` (sem `--sheet`), o script atualiza **somente a(s) aba(s) que mudaram** no JSON.

Se quiser forçar regenerar **todas** as abas a cada mudança:

```powershell
& "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\.venv\Scripts\python.exe" "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\excel_mapping\generate_mapping_excel.py" --watch --watch-all
```

Se quiser atualizar uma aba específica manualmente:

```powershell
& "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\.venv\Scripts\python.exe" "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\excel_mapping\generate_mapping_excel.py" --sheet "Cable 11 and 12"
```
