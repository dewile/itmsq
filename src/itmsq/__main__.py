"""
ITMS Service Broker test runner.
Run with: python -m itmsq
"""

from itmsq.taric import TaricBrokerError, pretty_xml, receive_response, send_request

# ── Connection ────────────────────────────────────────────────────────────────
# Any of the three servers can be used:
#   HA listener (recommended): Ncts5-nddb-te.carina.rs        10.0.131.43
#   Node 1 (direct):           Ncts5-nddb1-te\NCTS5_NDDB1_TE  10.0.131.41
#   Node 2 (direct):           Ncts5-nddb2-te\NCTS5_NDDB2_TE  10.0.131.42
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=127.0.0.1;"
    "DATABASE=SBMQ;"
    "UID=ais;"
    "PWD=ais;"
)

# ── Caller (initiator) — must exist in SBMQ; see taric.py header for setup SQL
INITIATOR_SERVICE = "AISIntTest"
INITIATOR_QUEUE = "[aisinttest].[AISIntQueue]"

# ── TC01 TaricRequest (GoodsCode 010229210080, Import, Country CA) ────────────
TC01_REQUEST = """
<TaricRequest>
  <Version>1.0</Version>
  <GUID>__auto__</GUID>
  <Header>
    <SADType>I</SADType>
    <Language>EN</Language>
  </Header>
  <Goods>
    <GoodsItemNumber>1</GoodsItemNumber>
    <GoodsShipment>1</GoodsShipment>
    <CalcDate>2026-05-01</CalcDate>
    <GoodsCode>010229210080</GoodsCode>
    <Country>CA</Country>
    <Preference>100</Preference>
    <CustomsProcedure>4000</CustomsProcedure>
    <Payment>
      <BasePrice>
        <BasePriceType>C</BasePriceType>
        <PriceAmount>100000</PriceAmount>
        <Currency>RSD</Currency>
      </BasePrice>
    </Payment>
  </Goods>
</TaricRequest>
"""


def main() -> None:
    handle = send_request(CONNECTION_STRING, INITIATOR_SERVICE, TC01_REQUEST)

    try:
        msg_type, msg_body = receive_response(CONNECTION_STRING, INITIATOR_QUEUE, handle)
    except TaricBrokerError as e:
        print(f"[ERROR]  {e}")
        return

    if msg_body:
        print(f"\n[RECV]   Message type: {msg_type}")
        print("[RECV]   Response body:")
        print(pretty_xml(msg_body))
    else:
        print("[RECV]   No response received within timeout.")


if __name__ == "__main__":
    main()
