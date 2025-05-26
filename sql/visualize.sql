/* Looker Link to visualizations
  https://lookerstudio.google.com/reporting/4444203a-0d6b-4469-8bb9-9f21c7ce93d6
*/

/*
  PAGE 1
  Datasource : noorah.noora_data.activation_rate
  Metric : Activation Rate -> Active Users / Total Users
    Total users: Anyone involved in communication with the bot within the week
    Active users: Users who initiated conversation (inbound) within the week
*/
SELECT
FORMAT_DATE('%Y-%W', DATE(inserted_at)) AS week,
COUNT(DISTINCT CASE
	WHEN direction = 'inbound' THEN masked_from_addr
	WHEN direction = 'outbound' THEN masked_addressees
END) AS total_users,
COUNT(DISTINCT CASE
	WHEN direction = 'inbound' THEN masked_from_addr
END) AS active_users
FROM `noorah.noora_data.message_status`
WHERE DATE(inserted_at) >= '2023-07-01'
GROUP BY week
ORDER BY week


/* 
  PAGE 2
  Datasource : noorah.noora_data.quick_analytics
  Metrics :
    1. weekly read messages of non-failed messages
    2. Time Hour Distribution - Read messages within n hours of delivery
    3. Total Messages by Status - Weekly
*/
WITH status_exploded AS (
  SELECT
    CAST(FORMAT_DATE('%Y%W', DATE(inserted_at)) AS int64) AS week,
    uuid,
    status_label,
    timestamp_value
  FROM `noorah.noora_data.message_status`,
  UNNEST([
    STRUCT('sent' AS status_label, sent_at AS timestamp_value),
    STRUCT('delivered' AS status_label, delivered_at),
    STRUCT('read' AS status_label, read_at),
    STRUCT('failed' AS status_label, failed_at),
    STRUCT('deleted' AS status_label, deleted_at)
  ])
  WHERE direction = 'outbound'
  AND inserted_at >= '2024-01-01'
  AND timestamp_value IS NOT NULL
)

SELECT
  '1' as query_type,
  CAST(FORMAT_DATE('%Y%W', DATE(inserted_at)) AS int64) AS week,
  '' As status,
  count(distinct case when read_at is not null then uuid end)/count(distinct uuid) as read_msgs_of_non_failed,
FROM `noorah.noora_data.message_status`
where direction = 'outbound'
and inserted_at >= '2024-01-01'
and sent_at is not null
group by 1,2,3

union all

SELECT
  '2' as query_type,
  timestamp_diff(read_at, sent_at, hour) as time_gap,
  '' As status,
  count(distinct uuid) as messages
FROM `noorah.noora_data.message_status`
where direction = 'outbound'
and sent_at is not null and read_at is not null
group by 1,2,3

union all

SELECT
  '3' as query_type,
  week,
  status_label AS status,
  COUNT(distinct uuid) AS message_count
FROM status_exploded
GROUP BY 1,2,3
ORDER BY 1,2,3;