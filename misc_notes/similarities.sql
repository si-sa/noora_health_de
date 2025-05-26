-- CREATE TEMP FUNCTION jaccard_bigram(str1 STRING, str2 STRING)
-- RETURNS FLOAT64
-- LANGUAGE js AS """
--   // Handle null or undefined inputs
--   if (!str1) str1 = '';
--   if (!str2) str2 = '';
  
--   str1 = str1.toLowerCase();
--   str2 = str2.toLowerCase();

--   function bigrams(s) {
--     let bg = new Set();
--     for (let i = 0; i < s.length - 1; i++) {
--       bg.add(s.substring(i, i+2));
--     }
--     return bg;
--   }

--   const b1 = bigrams(str1);
--   const b2 = bigrams(str2);

--   const intersection = new Set([...b1].filter(x => b2.has(x)));
--   const union = new Set([...b1, ...b2]);

--   if (union.size === 0) return 0.0;
--   return intersection.size / union.size;
-- """;


-- -- Detect similar content messages using Jaccard similarity of bigrams
-- SELECT
--   a.uuid AS uuid_1, 
--   b.uuid AS uuid_2, 
--   a.content AS content_1, 
--   b.content AS content_2,
--   jaccard_bigram(a.content, b.content) AS similarity,
--   a.inserted_at AS time_1,
--   b.inserted_at AS time_2
-- FROM (SELECT * FROM `noorah.noora_data.message_status` WHERE direction = 'inbound') a
-- JOIN (SELECT * FROM `noorah.noora_data.message_status` WHERE direction = 'inbound') b
--   ON a.uuid != b.uuid
--   AND a.masked_from_addr = b.masked_from_addr
--   AND a.inserted_at < b.inserted_at
--   AND TIMESTAMP_DIFF(b.inserted_at, a.inserted_at, SECOND) <= 60
-- WHERE 1=1
-- -- AND jaccard_bigram(a.content, b.content) >= 0.6
-- -- AND jaccard_bigram(a.content, b.content) < 1
-- ORDER BY similarity DESC;


CREATE TEMP FUNCTION jaccard(a STRING, b STRING)
RETURNS FLOAT64
LANGUAGE js AS """
  // Convert nulls to empty strings to avoid errors
  const strA = (a || '').toLowerCase();
  const strB = (b || '').toLowerCase();

  const setA = new Set(strA.split(/\\W+/));
  const setB = new Set(strB.split(/\\W+/));
  const intersection = new Set([...setA].filter(x => setB.has(x)));
  const union = new Set([...setA, ...setB]);
  return intersection.size / union.size;
""";

-- Detect similar content messages using Jaccard similarity of bigrams
SELECT
  a.uuid AS uuid_1, 
  b.uuid AS uuid_2, 
  a.content AS content_1, 
  b.content AS content_2,
  jaccard(a.content, b.content) AS similarity,
  a.inserted_at AS time_1,
  b.inserted_at AS time_2
FROM (select * from `noorah.noora_data.message_status` where content is not null) a
JOIN (select * from `noorah.noora_data.message_status` where content is not null) b
  ON a.uuid != b.uuid
  AND a.masked_from_addr = b.masked_from_addr
  AND a.masked_addressees = b.masked_addressees
  AND a.inserted_at < b.inserted_at
  AND TIMESTAMP_DIFF(b.inserted_at, a.inserted_at, SECOND) <= 600
WHERE 1=1
-- AND jaccard(a.content, b.content) >= 0.6
-- AND jaccard(a.content, b.content) < 1
ORDER BY similarity DESC;

-- all uuid_2 are the duplicate content. write a query to add a flag is_duplicate column to `noorah.noora_data.message_status` such that the all these uuid_2 values have false and rest as true.
-- Set is_duplicate = FALSE for known uuid_2s (not duplicates)
UPDATE `noorah.noora_data.message_status`
SET is_duplicate = FALSE
WHERE uuid IN (
  SELECT DISTINCT b.uuid
  FROM (select * from `noorah.noora_data.message_status` where content is not null) a
  JOIN (select * from `noorah.noora_data.message_status` where content is not null) b
  ON a.uuid != b.uuid
  AND a.masked_from_addr = b.masked_from_addr
  AND a.masked_addressees = b.masked_addressees
  AND a.inserted_at < b.inserted_at
  AND TIMESTAMP_DIFF(b.inserted_at, a.inserted_at, SECOND) <= 600
  WHERE 1=1
  AND jaccard(a.content, b.content) = 1
);

-- Set is_duplicate = TRUE for all other rows
UPDATE `noorah.noora_data.message_status`
SET is_duplicate = TRUE
WHERE is_duplicate IS NULL;



-- SELECT * FROM `noorah._8b3e85c3c1468f971dc88ae1d42affe62522a3f1.anona4ffe58c_30a9_4da2_baf8_7a1a9d914119` where similarity = 1

-- spread of duplicate messages sent from same sender to same receiver within 1 to 10 mins
SELECT TIMESTAMP_DIFF(time_2, time_1, minute), count(1)
FROM `noorah._8b3e85c3c1468f971dc88ae1d42affe62522a3f1.anona4ffe58c_30a9_4da2_baf8_7a1a9d914119` where similarity = 1
group by 1 order by 1;


SELECT *
FROM `noorah._8b3e85c3c1468f971dc88ae1d42affe62522a3f1.anona4ffe58c_30a9_4da2_baf8_7a1a9d914119` where similarity > 0.5;