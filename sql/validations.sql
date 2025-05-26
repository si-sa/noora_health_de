-- i. Multiple statuses with same exact timestamp for same message (dude to possible ingestion error?)
    SELECT message_uuid, status, inserted_at, COUNT(*) AS cnt
    FROM `noorah.noora_data.raw_statuses`
    GROUP BY message_uuid, status, inserted_at
    HAVING cnt > 1;

-- ii. Status sequences out of expected order
    -- Check if a message has a status that shouldn't appear before an earlier stage (e.g., delivered before sent).
    -- Validation: Check for out-of-order status transitions

    WITH first_status_per_type AS (
        SELECT
            message_uuid,
            MIN(CASE WHEN status = 'sent' THEN inserted_at END) AS sent_time,
            MIN(CASE WHEN status = 'delivered' THEN inserted_at END) AS delivered_time,
            MIN(CASE WHEN status = 'read' THEN inserted_at END) AS read_time,
            MIN(CASE WHEN status = 'deleted' THEN inserted_at END) AS deleted_time
        FROM `noorah.noora_data.raw_statuses`
        GROUP BY message_uuid
    )

    SELECT *
    FROM first_status_per_type
    WHERE 
    -- 1. Delivered before Sent
    delivered_time IS NOT NULL AND sent_time IS NOT NULL AND delivered_time < sent_time

    -- 2. Read before Delivered
    OR read_time IS NOT NULL AND delivered_time IS NOT NULL AND read_time < delivered_time

    -- 3. Deleted before Sent
    OR deleted_time IS NOT NULL AND sent_time IS NOT NULL AND deleted_time < sent_time

    -- 4. Deleted before Delivered
    OR deleted_time IS NOT NULL AND delivered_time IS NOT NULL AND deleted_time < delivered_time;

-- iii. Messages with invalid directions or statuses
    -- (Valid direction should be either inbound or outbound.)
    -- 
    SELECT status, count(1)
    FROM `noorah.noora_data.raw_statuses`
    group by 1;

    SELECT direction, count(1)
    FROM `noorah.noora_data.raw_messages`
    group by 1;

-- Other checks :
-- - Agent to agent messages? 
    -- not sure about whihc ids are agent ids to cross verify
    -- But we can check if an id is present in both masked_addressees and masked_from_addr for same inbound or outbound messages
-- - Messages with invalid phone number format (not available in data)
-- - foreign keys missing in OG table