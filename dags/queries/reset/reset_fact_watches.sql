DROP TABLE IF EXISTS master.fact_watches;
CREATE TABLE master.fact_watches
(
    SK_CustomerID         INT64 NOT NULL, --Customer associated with watch list
    SK_SecurityID         INT64 NOT NULL, -- Security listed on watch list
    SK_DateID_DatePlaced  INT64 NOT NULL, -- Date the watch list item was added
    SK_DateID_DateRemoved INT64,          -- the watch list item was removed
    BatchID               INT64 NOT NULL  -- Batch ID when this record was inserted
);