"""
Taric RS Service Broker client.

Initiator setup SQL (run once in SBMQ if a test caller service doesn't exist):
--------------------------------------------------------------
CREATE QUEUE [dbo].[TestAISCallerQueue];
CREATE SERVICE [TestAISCaller]
    ON QUEUE [dbo].[TestAISCallerQueue];
--------------------------------------------------------------
"""

import uuid
import xml.dom.minidom

import pyodbc

ENVELOPE_NS = "http://www.aquasoft.cz/namespaces/envelope/ver4"
MSG_TYPE = f"[{ENVELOPE_NS}]"
TARIC_CONTRACT = f"[{ENVELOPE_NS}/normal/sync]"
TARIC_SERVICE = "Itmstaric"
RECEIVE_TIMEOUT = 15000  # ms

SB_MSG_TYPE_RESPONSE = ENVELOPE_NS
SB_MSG_TYPE_ERROR = "http://schemas.microsoft.com/SQL/ServiceBroker/Error"
SB_MSG_TYPE_TIMEOUT = "http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer"


class TaricBrokerError(Exception):
    """Raised when Service Broker returns an Error or Timeout system message."""


def build_request_xml(taric_request_xml: str, request_guid: str | None = None) -> str:
    """Wrap a TaricRequest body in the Service Broker envelope."""
    if request_guid is None:
        request_guid = str(uuid.uuid4())
    # Strip XML declaration if present — envelope is the document root
    body = taric_request_xml.strip()
    if body.startswith("<?xml"):
        body = body[body.index("?>") + 2:].strip()
    # Replace __auto__ placeholder with a real UUID
    body = body.replace("__auto__", request_guid)
    return (
        f'<env:Envelope msgtype="TaricRequest" xmlns:env="{ENVELOPE_NS}">'
        f"<env:Data>{body}</env:Data>"
        f"</env:Envelope>"
    )


def send_request(
    connection_string: str,
    initiator_service: str,
    request_xml: str,
) -> str:
    """
    Open a dialog to Itmstaric, send the envelope-wrapped TaricRequest,
    and return the conversation handle.
    """
    envelope = build_request_xml(request_xml)

    conn = pyodbc.connect(connection_string, autocommit=False)
    cursor = conn.cursor()

    cursor.execute(
        f"""
        DECLARE @handle UNIQUEIDENTIFIER;

        BEGIN DIALOG CONVERSATION @handle
            FROM SERVICE ?
            TO SERVICE ?
            ON CONTRACT {TARIC_CONTRACT}
            WITH ENCRYPTION = OFF;

        SEND ON CONVERSATION @handle
            MESSAGE TYPE {MSG_TYPE}
            (CAST(? AS NVARCHAR(MAX)));

        SELECT @handle AS conversation_handle;
        """,
        initiator_service,
        TARIC_SERVICE,
        envelope,
    )

    row = cursor.fetchone()
    handle = str(row.conversation_handle)
    conn.commit()
    conn.close()

    print(f"[SEND]   → Itmstaric | handle: {handle}")
    return handle


def receive_response(
    connection_string: str,
    initiator_queue: str,
    conversation_handle: str,
) -> tuple[str | None, str | None]:
    """
    Wait for a reply on the initiator queue for the given conversation.
    Returns (msg_type, msg_body) or (None, None) on timeout.
    """
    conn = pyodbc.connect(connection_string, autocommit=False)
    cursor = conn.cursor()

    print(f"[RECV]   Waiting on {initiator_queue} (timeout {RECEIVE_TIMEOUT}ms)...")

    cursor.execute(
        f"""
        DECLARE @handle    UNIQUEIDENTIFIER;
        DECLARE @msg_type  NVARCHAR(256);
        DECLARE @msg_body  NVARCHAR(MAX);

        WAITFOR (
            RECEIVE TOP(1)
                @handle   = conversation_handle,
                @msg_type = message_type_name,
                @msg_body = CAST(message_body AS NVARCHAR(MAX))
            FROM {initiator_queue}
            WHERE conversation_handle = ?
        ), TIMEOUT ?;

        IF @handle IS NOT NULL
            END CONVERSATION @handle;

        SELECT @handle AS handle, @msg_type AS msg_type, @msg_body AS msg_body;
        """,
        conversation_handle,
        RECEIVE_TIMEOUT,
    )

    row = cursor.fetchone()
    conn.commit()
    conn.close()

    if not row or not row.handle:
        return None, None

    if row.msg_type == SB_MSG_TYPE_ERROR:
        raise TaricBrokerError(f"Service Broker conversation error: {row.msg_body}")
    if row.msg_type == SB_MSG_TYPE_TIMEOUT:
        raise TaricBrokerError("Service Broker conversation timeout: Taric did not respond in time.")

    return row.msg_type, row.msg_body


def pretty_xml(xml_string: str) -> str:
    try:
        return xml.dom.minidom.parseString(xml_string.encode()).toprettyxml(indent="  ")
    except Exception:
        return xml_string
