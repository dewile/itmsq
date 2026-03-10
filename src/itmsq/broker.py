import pyodbc

RECEIVE_TIMEOUT = 5000  # ms


def send_message(connection_string: str, message_text: str, *, initiator_service: str, target_service: str, contract: str, request_msg_type: str) -> str:
    """Open a conversation from Initiator → Target, send a request, return the conversation handle."""
    conn = pyodbc.connect(connection_string, autocommit=False)
    cursor = conn.cursor()

    cursor.execute(
        """
        DECLARE @handle UNIQUEIDENTIFIER;

        BEGIN DIALOG CONVERSATION @handle
            FROM SERVICE ?
            TO SERVICE ?
            ON CONTRACT ?
            WITH ENCRYPTION = OFF;

        SEND ON CONVERSATION @handle
            MESSAGE TYPE ?
            (?);

        SELECT @handle AS conversation_handle;
        """,
        initiator_service,
        target_service,
        contract,
        request_msg_type,
        message_text,
    )

    row = cursor.fetchone()
    conversation_handle = str(row.conversation_handle)
    conn.commit()
    conn.close()

    print(f"[SENDER]   Message sent: '{message_text}'")
    print(f"[SENDER]   Conversation handle: {conversation_handle}")
    return conversation_handle


def process_target_queue(connection_string: str, *, request_msg_type: str, reply_msg_type: str) -> None:
    """Read a request from TargetQueue, send a reply, and end the target-side conversation."""
    conn = pyodbc.connect(connection_string, autocommit=False)
    cursor = conn.cursor()

    print(f"\n[TARGET]   Waiting for messages on TargetQueue ({RECEIVE_TIMEOUT}ms timeout)...")

    cursor.execute(
        """
        DECLARE @handle       UNIQUEIDENTIFIER;
        DECLARE @msg_type     NVARCHAR(256);
        DECLARE @msg_body     NVARCHAR(MAX);

        WAITFOR (
            RECEIVE TOP(1)
                @handle   = conversation_handle,
                @msg_type = message_type_name,
                @msg_body = CAST(message_body AS NVARCHAR(MAX))
            FROM TargetQueue
        ), TIMEOUT ?;

        IF @handle IS NOT NULL AND @msg_type = ?
        BEGIN
            SEND ON CONVERSATION @handle
                MESSAGE TYPE ?
                ('Reply: got your message \u2192 ' + @msg_body);

            END CONVERSATION @handle;
        END

        SELECT @handle AS handle, @msg_type AS msg_type, @msg_body AS msg_body;
        """,
        RECEIVE_TIMEOUT,
        request_msg_type,
        reply_msg_type,
    )

    row = cursor.fetchone()
    conn.commit()
    conn.close()

    if row and row.handle:
        print(f"[TARGET]   Received request : '{row.msg_body}'")
        print("[TARGET]   Reply sent, conversation ended.")
    else:
        print("[TARGET]   No message received within timeout.")


def receive_reply(connection_string: str, conversation_handle: str) -> None:
    """Wait for a reply on InitiatorQueue and end the initiator-side conversation."""
    conn = pyodbc.connect(connection_string, autocommit=False)
    cursor = conn.cursor()

    print(f"\n[RECEIVER] Waiting for reply on InitiatorQueue ({RECEIVE_TIMEOUT}ms timeout)...")

    cursor.execute(
        """
        DECLARE @handle       UNIQUEIDENTIFIER;
        DECLARE @msg_type     NVARCHAR(256);
        DECLARE @msg_body     NVARCHAR(MAX);

        WAITFOR (
            RECEIVE TOP(1)
                @handle   = conversation_handle,
                @msg_type = message_type_name,
                @msg_body = CAST(message_body AS NVARCHAR(MAX))
            FROM InitiatorQueue
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

    if row and row.handle:
        print(f"[RECEIVER] Reply received  : '{row.msg_body}'")
        print(f"[RECEIVER] Message type    : {row.msg_type}")
    else:
        print("[RECEIVER] No reply received within timeout.")
