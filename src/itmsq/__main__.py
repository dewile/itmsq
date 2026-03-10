"""
SQL Server Service Broker - Python Demo
Run with: python -m itmsq

Setup SQL (run once in SSMS or similar):
--------------------------------------------------------------
-- Enable Service Broker on your DB (if not already enabled)
ALTER DATABASE YourDatabase SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;

-- Message type
CREATE MESSAGE TYPE [//Demo/RequestMessage]  VALIDATION = NONE;
CREATE MESSAGE TYPE [//Demo/ReplyMessage]    VALIDATION = NONE;

-- Contract
CREATE CONTRACT [//Demo/Contract]
    ([//Demo/RequestMessage] SENT BY INITIATOR,
     [//Demo/ReplyMessage]   SENT BY TARGET);

-- Queues
CREATE QUEUE InitiatorQueue;
CREATE QUEUE TargetQueue;

-- Services
CREATE SERVICE [//Demo/InitiatorService]
    ON QUEUE InitiatorQueue ([//Demo/Contract]);
CREATE SERVICE [//Demo/TargetService]
    ON QUEUE TargetQueue ([//Demo/Contract]);
--------------------------------------------------------------
"""

import time

from itmsq.broker import process_target_queue, receive_reply, send_message

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=YourDatabase;"
    "UID=your_user;"
    "PWD=your_password;"
    # For Windows auth, replace UID/PWD with: "Trusted_Connection=yes;"
)

INITIATOR_SERVICE = "//Demo/InitiatorService"
TARGET_SERVICE = "//Demo/TargetService"
CONTRACT = "//Demo/Contract"
REQUEST_MSG_TYPE = "//Demo/RequestMessage"
REPLY_MSG_TYPE = "//Demo/ReplyMessage"


def main() -> None:
    handle = send_message(
        CONNECTION_STRING,
        "Hello from Python!",
        initiator_service=INITIATOR_SERVICE,
        target_service=TARGET_SERVICE,
        contract=CONTRACT,
        request_msg_type=REQUEST_MSG_TYPE,
    )

    time.sleep(0.5)

    process_target_queue(
        CONNECTION_STRING,
        request_msg_type=REQUEST_MSG_TYPE,
        reply_msg_type=REPLY_MSG_TYPE,
    )

    time.sleep(0.5)

    receive_reply(CONNECTION_STRING, handle)


if __name__ == "__main__":
    main()
