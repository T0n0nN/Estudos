import sys
import asyncio
from pysnmp.hlapi.v3arch.asyncio import (
    SnmpEngine,
    CommunityData,
    UdpTransportTarget,
    ContextData,
    ObjectType,
    ObjectIdentity,
    walk_cmd,
)

# Usage:
#   python snmp_test_trunk.py 10.104.136.1 gyplant
#
# This tests Cisco VTP trunk table OIDs used by the Zabbix discovery.

TRUNK_STATUS_OID = "1.3.6.1.4.1.9.9.46.1.6.1.1.14"  # vlanTrunkPortDynamicStatus
TRUNK_IFINDEX_OID = "1.3.6.1.4.1.9.9.46.1.6.1.1.1"  # vlanTrunkPortIfIndex


async def walk(host: str, community: str, oid: str, limit: int = 30):
    engine = SnmpEngine()
    target = await UdpTransportTarget.create((host, 161), timeout=2, retries=1)
    auth = CommunityData(community, mpModel=1)  # v2c

    count = 0
    async for (errorIndication, errorStatus, errorIndex, varBinds) in walk_cmd(
        engine,
        auth,
        target,
        ContextData(),
        ObjectType(ObjectIdentity(oid)),
        lexicographicMode=False,
    ):
        if errorIndication:
            raise RuntimeError(str(errorIndication))
        if errorStatus:
            raise RuntimeError(
                f"{errorStatus.prettyPrint()} at {errorIndex and varBinds[int(errorIndex) - 1][0] or '?'}"
            )

        for name, val in varBinds:
            print(f"{name.prettyPrint()} = {val.prettyPrint()}")
            count += 1
            if count >= limit:
                return


async def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "10.104.136.1"
    community = sys.argv[2] if len(sys.argv) > 2 else "gyplant"

    print(f"Walking {TRUNK_STATUS_OID} on {host}...")
    await walk(host, community, TRUNK_STATUS_OID)
    print("\nWalking", TRUNK_IFINDEX_OID, "on", host, "...")
    await walk(host, community, TRUNK_IFINDEX_OID)


if __name__ == "__main__":
    asyncio.run(main())
