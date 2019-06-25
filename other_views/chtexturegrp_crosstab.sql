SELECT * 
  FROM crosstab (
	   'SELECT chkey, row_num, texdesc
	      FROM (
		       SELECT chkey, rvindicator, texdesc, 
			          Row_Number() Over(PARTITION BY chkey ORDER BY rvindicator DESC, texdesc ASC) AS row_num
		         FROM chtexturegrp) AS a
	     ORDER BY 1, 2')
    AS chtexturegrp_crosstab(chkey VARCHAR (30), texture_1 TEXT, texture_2 TEXT, texture_3 TEXT);