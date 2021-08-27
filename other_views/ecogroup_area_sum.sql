SELECT ecogroup, group_split[1] AS mlra,
       ((regexp_match(group_split[2], '\d+'))[1])::integer AS group_no,
       group_split[2] AS group_id,
       groupname, grouptype, pub_status, ecogroup_ha
  FROM (
       SELECT ecogroup, CASE WHEN grouptype = 'DRG' THEN regexp_split_to_array(ecogroup, '\s') ELSE NULL END AS group_split,
              groupname, grouptype, pub_status, ecogroup_ha
         FROM ( 
              SELECT ecogroup, groupname, grouptype, pub_status, sum(ecogroup_ha) As ecogroup_ha
                FROM soil.ecogroup_area
               WHERE ecogroup_ha IS NOT NULL
               GROUP BY ecogroup, groupname, grouptype, pub_status) AS a) AS b
 ORDER BY grouptype, mlra, group_no, group_id