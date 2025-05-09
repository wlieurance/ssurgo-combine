CREATE VIEW copmgrp_first AS
WITH pm_rn AS (
SELECT *,
       row_number() over(partition by cokey order by rvindicator desc, copmgrpkey) rn
  FROM copmgrp
)

SELECT * FROM pm_rn WHERE rn = 1;