3. Check the transformed data:
    a. Write a query to detect and flag duplicate records based on identical content and similar inserted_at timestamps.
        
        content only inbound, from same adresser to adressee, in span of 1 min.
        identical?
            -> duplicate
            -> jaccardi score >= 0.8
                Challenges, letter transpositions like clear vs claer have diff scores
    
    b. Include at least three additional data validation queries to check for consistency, quality, etc. of the data.
        i. Multiple statuses with same exact timestamp for same message (possible ingestion error)
            SELECT message_uuid, status, COUNT(*) AS cnt, inserted_at
            FROM `noorah.noora_data.raw_statuses`
            GROUP BY message_uuid, status, inserted_at
            HAVING cnt > 1;
        ii. Messages with invalid phone number format (nott available in data)
        iii. Status sequences out of expected order
            -- Messages marked as 'read' before being 'delivered' or 'sent'
            Check if a message has a status that shouldn't appear before an earlier stage (e.g., delivered before sent).
            WITH ordered_status AS (
                SELECT
                    message_uuid,
                    status,
                    inserted_at,
                    RANK() OVER (PARTITION BY message_uuid ORDER BY inserted_at) AS rank
                FROM `noorah.noora_data.raw_statuses`
            )
            SELECT *
            FROM ordered_status
            WHERE status = 'delivered' AND rank = 1;
        iv. Agent to agent messages?
        v. Messages with invalid directions
            (Valid direction should be either inbound or outbound.)
            SELECT *
            FROM `noorah.noora_data.message_status`
            WHERE direction NOT IN ('inbound', 'outbound');

        