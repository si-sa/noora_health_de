/*
  Purpose:
    This User Defined Function (UDF) calculates the Jaccard similarity between two strings in BigQuery. It’s useful for identifying similar message content by comparing sets of words (tokens).

    We can optimise and add a jaccard_bigram function where instead of work comparision, we compare every 2 letter combinations for the 2 sentences. Adding an additional bad letter in a word doesn't completely rule out the similarity in such cases
*/
CREATE OR REPLACE FUNCTION `noorah.noora_data.jaccard`(a STRING, b STRING)
RETURNS FLOAT64
LANGUAGE js AS """
  const strA = (a || '').toLowerCase();
  const strB = (b || '').toLowerCase();

  const tokensA = new Set(strA.split(/\\W+/));
  const tokensB = new Set(strB.split(/\\W+/));

  const intersection = new Set([...tokensA].filter(x => tokensB.has(x)));
  const union = new Set([...tokensA, ...tokensB]);

  return union.size === 0 ? 0 : intersection.size / union.size;
""";

-- Add is_duplicate column if it doesn't exist
ALTER TABLE `noorah.noora_data.message_status`
ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN;

/* Then: set is_duplicate = FALSE for known uuid_2 values.
  duplicate messages are those which are sent from same sender to same receiver within 10 mins of the first message having same the content (jaccard score = 1). 
  Put jaccard score slightly below 1 for selecting similar texts too
*/
UPDATE `noorah.noora_data.message_status`
SET is_duplicate = FALSE
WHERE uuid IN (
  SELECT DISTINCT b.uuid
  FROM (
    SELECT
      a.uuid AS uuid_1, 
      b.uuid AS uuid_2,
      a.content AS content_1, 
      b.content AS content_2,
      jaccard(a.content, b.content) AS similarity,
      a.inserted_at AS time_1,
      b.inserted_at AS time_2
    FROM (SELECT * FROM `noorah.noora_data.message_status` WHERE content IS NOT NULL) a
    JOIN (SELECT * FROM `noorah.noora_data.message_status` WHERE content IS NOT NULL) b
      ON a.uuid != b.uuid
      AND a.masked_from_addr = b.masked_from_addr
      AND a.masked_addressees = b.masked_addressees
      AND a.inserted_at < b.inserted_at
      AND TIMESTAMP_DIFF(b.inserted_at, a.inserted_at, SECOND) <= 600
  )
  WHERE similarity = 1
);

-- Finally: set remaining rows to TRUE (duplicates)
UPDATE `noorah.noora_data.message_status`
SET is_duplicate = TRUE
WHERE is_duplicate IS NULL;


/*
-- SELECT * FROM `noorah._8b3e85c3c1468f971dc88ae1d42affe62522a3f1.anona4ffe58c_30a9_4da2_baf8_7a1a9d914119` where similarity = 1

-- spread of duplicate messages sent from same sender to same receiver within 1 to 10 mins
SELECT TIMESTAMP_DIFF(time_2, time_1, minute), count(1)
FROM `noorah._8b3e85c3c1468f971dc88ae1d42affe62522a3f1.anona4ffe58c_30a9_4da2_baf8_7a1a9d914119` where similarity = 1
group by 1 order by 1;

-- raw records with higher similarity but not the same content
SELECT *
FROM `noorah._8b3e85c3c1468f971dc88ae1d42affe62522a3f1.anona4ffe58c_30a9_4da2_baf8_7a1a9d914119` where similarity > 0.5 and similarity < 1;
*/