/*
  Add a sent_at, delivered_at, read_at, failed_at, deleted_at timestamps for a message based on values in raw_stauses table to generate a transformed table comtaining one record for each message.

  We have seen some duplicates on message_uuid but below query removed those on timestamps of various status.
*/
CREATE TABLE `noorah.noora_data.message_status` AS
WITH status_pivot AS (
  SELECT
    message_uuid,
    MAX(IF(status = 'sent', timestamp, NULL)) AS sent_at,
    MAX(IF(status = 'delivered', timestamp, NULL)) AS delivered_at,
    MAX(IF(status = 'read', timestamp, NULL)) AS read_at,
    MAX(IF(status = 'failed', timestamp, NULL)) AS failed_at,
    MAX(IF(status = 'deleted', timestamp, NULL)) AS deleted_at
  FROM `noorah.noora_data.raw_statuses`
  GROUP BY message_uuid
)
SELECT
  m.*,
  sp.sent_at,
  sp.delivered_at,
  sp.read_at,
  sp.failed_at,
  sp.deleted_at
FROM `noorah.noora_data.raw_messages` m
LEFT JOIN status_pivot sp
  ON m.uuid = sp.message_uuid;