WITH comp AS (
SELECT a.areasymbol, b.areasymbol, b.musym, b.mukey, b.area_ha,
       c.cokey, c.compname, c.compnamelong, c.comppct_r, c.comprank,
       (cast(c.comppct_r AS REAL)/100.0) * b.area_ha comp_ha,
       d.ecoclasstypename, d.ecoclassref, d.ecoclassid, d.ecoclassname, 
       d.coecoclasskey, d.ecoclassid_std, d.ecoclassname_std
  FROM legend a
 INNER JOIN mupolygon b ON a.areasymbol = b.areasymbol
 INNER JOIN component_mapunit_ranked c ON b.mukey = c.mukey
 INNER JOIN coecoclass_codominant d ON c.cokey = d.cokey

)
SELECT ecoclassid_std, max(ecoclassid) ecoclassid, max(ecoclassname) ecoclassname, 
       max(ecoclassname_std) ecoclassname_std,
       compname, round(sum(comp_ha), 1) area_ha
  FROM comp
 GROUP BY ecoclassid_std, compname
 ORDER BY ecoclassid_std, area_ha DESC;
