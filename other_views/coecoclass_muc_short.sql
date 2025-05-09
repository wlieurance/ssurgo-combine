WITH comp AS (
SELECT a.areasymbol, b.areasymbol, b.musym, b.mukey, b.area_ha,
       c.cokey, c.compname, c.compnamelong, c.comppct_r, c.comprank,
       (cast(c.comppct_r AS REAL)/100.0) * b.area_ha comp_ha,
       d.ecoclasstypename, d.ecoclassref, d.ecoclassid, d.ecoclassname, 
       d.coecoclasskey, d.ecoclassid_std, d.ecoclassname_std
  FROM {schema}.legend a
 INNER JOIN {schema}.mupolygon b ON a.areasymbol = b.areasymbol
 INNER JOIN {schema}.component_mapunit_ranked c ON b.mukey = c.mukey
 INNER JOIN {schema}.coecoclass_codominant d ON c.cokey = d.cokey

), area_sum AS (
SELECT ecoclassid, ecoclassid_std, ecoclassname, ecoclassname_std,
       compname, sum(comp_ha) comp_ha
  FROM comp
 GROUP BY ecoclassid, ecoclassid_std, ecoclassname, ecoclassname_std, compname
)

SELECT ecoclassid_std, max(ecoclassid) ecoclassid, min(ecoclassname) ecoclassname, 
       min(ecoclassname_std) ecoclassname_std,
       group_concat(compname, ', ' ORDER BY comp_ha DESC) component,
       group_concat(cast(cast(comp_ha AS INTEGER) AS VARCHAR), ', ' ORDER BY comp_ha DESC) component_area_ha
  FROM area_sum
 GROUP BY ecoclassid_std;
