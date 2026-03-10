#!/usr/bin/env python3
import subprocess
import re
import json
import sys

# Permite passar host e community por argumento
host = sys.argv[1] if len(sys.argv) >= 2 and sys.argv[1] else "10.104.136.1"
community = sys.argv[2] if len(sys.argv) >= 3 and sys.argv[2] else "gyplant"

oid = "1.2.840.10006.300.43.1.2.1.1.13"

# Executa snmpwalk
result = subprocess.run(
    ["snmpwalk", "-v2c", "-c", community, host, oid],
    capture_output=True,
    text=True,
)

if result.returncode != 0:
    print(json.dumps({"data": []}))
    sys.exit(0)

# Filtra ifIndex únicos
seen = set()
data = []
for line in result.stdout.splitlines():
    match = re.search(r'\.(\d+) = INTEGER: (\d+)', line)
    if match:
        portchannel_index = match.group(1)
        ifindex = match.group(2)
        if ifindex not in seen:
            seen.add(ifindex)
            data.append({
                "{#PORTCHANNELINDEX}": portchannel_index,
                "{#SNMPINDEX}": ifindex
            })

print(json.dumps({"data": data}))
