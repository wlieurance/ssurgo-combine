------- LEVEL 0 -------
CREATE TABLE IF NOT EXISTS {schema}.mdstatdommas (
    domainname VARCHAR (40) PRIMARY KEY,
    domainmaxlen SMALLINT NOT NULL);
    
CREATE TABLE IF NOT EXISTS {schema}.mdstattabs (
    tabphyname VARCHAR (30) PRIMARY KEY,
    tablogname VARCHAR (30) UNIQUE NOT NULL,
    tablabel VARCHAR (80) UNIQUE NOT NULL,
    tabdesc TEXT NOT NULL,
    iefilename VARCHAR (30) UNIQUE NOT NULL);
    
CREATE TABLE IF NOT EXISTS {schema}.month (
    monthseq SMALLINT UNIQUE NOT NULL,
    monthname VARCHAR (9) PRIMARY KEY);
    
INSERT INTO {schema}.month (monthseq, monthname) VALUES
(1, 'January'),
(2, 'February'),
(3, 'March'),
(4, 'April'),
(5, 'May'),
(6, 'June'),
(7, 'July'),
(8, 'August'),
(9, 'September'),
(10, 'October'),
(11, 'November'),
(12, 'December')
ON CONFLICT DO NOTHING;

-- acts as a linchpin to cascade delete all other data
CREATE TABLE IF NOT EXISTS {schema}.sacatalog (
    areasymbol VARCHAR (20) UNIQUE NOT NULL,
    areaname VARCHAR (135) NOT NULL,
    saversion INTEGER NOT NULL,
    saverest DATE NOT NULL,
    tabularversion INTEGER NOT NULL,
    tabularverest DATE NOT NULL,
    tabnasisexportdate DATE NOT NULL,
    tabcertstatus VARCHAR (254),
    tabcertstatusdesc TEXT,
    fgdcmetadata TEXT NOT NULL,
    sacatalogkey VARCHAR (30) PRIMARY KEY);
    
CREATE TABLE IF NOT EXISTS {schema}.sdvalgorithm (
    algorithmsequence SMALLINT UNIQUE NOT NULL,
    algorithmname VARCHAR (50) PRIMARY KEY,
    algorithminitials VARCHAR (3) UNIQUE NOT NULL,
    algorithmdescription TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS {schema}.sdvattribute (
    attributekey INTEGER PRIMARY KEY,
    attributename VARCHAR (60) NOT NULL,
    attributetablename VARCHAR (30) NOT NULL,
    attributecolumnname VARCHAR (30) NOT NULL,
    attributelogicaldatatype VARCHAR (20) NOT NULL,
    attributefieldsize SMALLINT,
    attributeprecision SMALLINT,
    attributedescription TEXT NOT NULL,
    attributeuom VARCHAR (60),
    attributeuomabbrev VARCHAR (30),
    attributetype VARCHAR (20) NOT NULL,
    nasisrulename VARCHAR (60),
    ruledesign SMALLINT,
    notratedphrase VARCHAR (254),
    mapunitlevelattribflag SMALLINT NOT NULL,
    complevelattribflag SMALLINT NOT NULL,
    cmonthlevelattribflag SMALLINT NOT NULL,
    horzlevelattribflag SMALLINT NOT NULL,
    tiebreakdomainname VARCHAR (40),
    tiebreakruleoptionflag SMALLINT NOT NULL,
    tiebreaklowlabel VARCHAR (20),
    tiebreakhighlabel VARCHAR (20),
    tiebreakrule SMALLINT NOT NULL,
    resultcolumnname VARCHAR (10) NOT NULL,
    sqlwhereclause VARCHAR (255),
    primaryconcolname VARCHAR (30),
    pcclogicaldatatype VARCHAR (20),
    primaryconstraintlabel VARCHAR (30),
    secondaryconcolname VARCHAR (30),
    scclogicaldatatype VARCHAR (20),
    secondaryconstraintlabel VARCHAR (30),
    dqmodeoptionflag SMALLINT NOT NULL,
    depthqualifiermode VARCHAR (20),
    layerdepthtotop DOUBLE PRECISION,
    layerdepthtobottom DOUBLE PRECISION,
    layerdepthuom VARCHAR (20),
    monthrangeoptionflag SMALLINT NOT NULL,
    beginningmonth VARCHAR (9),
    endingmonth VARCHAR (9),
    horzaggmeth VARCHAR (30),
    interpnullsaszerooptionflag SMALLINT NOT NULL,
    interpnullsaszeroflag SMALLINT NOT NULL,
    nullratingreplacementvalue VARCHAR (254),
    basicmodeflag SMALLINT NOT NULL,
    maplegendkey INTEGER NOT NULL,
    maplegendclasses SMALLINT,
    maplegendxml TEXT NOT NULL,
    nasissiteid INTEGER NOT NULL,
    wlupdated DATE NOT NULL,
    algorithmname VARCHAR (50) NOT NULL,
    componentpercentcutoff SMALLINT,
    readytodistribute SMALLINT NOT NULL,
    effectivelogicaldatatype VARCHAR (20) NOT NULL);

CREATE TABLE IF NOT EXISTS {schema}.sdvfolder (
    foldersequence SMALLINT NOT NULL,
    foldername VARCHAR (80) NOT NULL,
    folderdescription TEXT NOT NULL,
    folderkey INTEGER PRIMARY KEY,
    parentfolderkey INTEGER,
    wlupdated DATE NOT NULL);
    
------- LEVEL 1 -------    
CREATE TABLE IF NOT EXISTS {schema}.featdesc (
    areasymbol VARCHAR (20) NOT NULL,
    spatialversion INTEGER NOT NULL,
    featsym VARCHAR (3) NOT NULL,
    featname VARCHAR (80) NOT NULL,
    featdesc TEXT NOT NULL,
    featkey VARCHAR (30) PRIMARY KEY,
    UNIQUE (areasymbol, featsym),
    FOREIGN KEY (areasymbol) REFERENCES {schema}.sacatalog(areasymbol) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.featline (
    areasymbol VARCHAR (20),
    spatialver DOUBLE PRECISION,
    featsym VARCHAR (3),
    featkey VARCHAR (30) PRIMARY KEY,
    length_m DOUBLE PRECISION,
    FOREIGN KEY (areasymbol) REFERENCES {schema}.sacatalog(areasymbol) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.featpoint (
    areasymbol VARCHAR (20),
    spatialver DOUBLE PRECISION,
    featsym VARCHAR (3),
    featkey VARCHAR (30) PRIMARY KEY,
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,
    FOREIGN KEY (areasymbol) REFERENCES {schema}.sacatalog(areasymbol) ON DELETE CASCADE);
    
CREATE TABLE IF NOT EXISTS {schema}.legend (
    areatypename VARCHAR (45) NOT NULL,
    areasymbol VARCHAR (20) NOT NULL,
    areaname VARCHAR (135),
    areaacres INTEGER,
    mlraoffice VARCHAR (254),
    legenddesc VARCHAR (60),
    ssastatus VARCHAR (254),
    mouagncyresp VARCHAR (254),
    projectscale INTEGER,
    cordate DATE,
    ssurgoarchived DATE,
    legendsuituse VARCHAR (254),
    legendcertstat VARCHAR (254),
    lkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (areasymbol) REFERENCES {schema}.sacatalog(areasymbol) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.mdstatdomdet (
    domainname VARCHAR (40) NOT NULL,
    choicesequence SMALLINT NOT NULL,
    choice VARCHAR (254) NOT NULL,
    choicedesc TEXT,
    choiceobsolete VARCHAR (3) NOT NULL,
    PRIMARY KEY (domainname, choicesequence),
    UNIQUE (domainname, choice),
    FOREIGN KEY (domainname) REFERENCES {schema}.mdstatdommas(domainname) ON DELETE CASCADE);
    
CREATE TABLE IF NOT EXISTS {schema}.mdstatidxmas (
    tabphyname VARCHAR (30) NOT NULL,
    idxphyname VARCHAR (30) NOT NULL,
    uniqueindex VARCHAR (3) NOT NULL,
    PRIMARY KEY (tabphyname, idxphyname),
    FOREIGN KEY (tabphyname) REFERENCES {schema}.mdstattabs(tabphyname) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.mdstatrshipmas (
    ltabphyname VARCHAR (30) NOT NULL,
    rtabphyname VARCHAR (30) NOT NULL,
    relationshipname VARCHAR (30) NOT NULL,
    cardinality VARCHAR (254) NOT NULL,
    mandatory VARCHAR (3) NOT NULL,
    PRIMARY KEY (ltabphyname, rtabphyname, relationshipname),
    FOREIGN KEY (ltabphyname) REFERENCES {schema}.mdstattabs(tabphyname) ON DELETE CASCADE,
    FOREIGN KEY (rtabphyname) REFERENCES {schema}.mdstattabs(tabphyname) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.mdstattabcols (
    tabphyname VARCHAR (30) NOT NULL,
    colsequence SMALLINT NOT NULL,
    colphyname VARCHAR (30) NOT NULL,
    collogname VARCHAR (30) NOT NULL,
    collabel VARCHAR (80) NOT NULL,
    logicaldatatype VARCHAR (254) NOT NULL,
    not_null VARCHAR (3) NOT NULL,
    fieldsize SMALLINT,
    precision SMALLINT,
    minimum DOUBLE PRECISION,
    maximum DOUBLE PRECISION,
    uom VARCHAR (60),
    domainname VARCHAR (40),
    coldesc TEXT NOT NULL,
    PRIMARY KEY (tabphyname, colsequence),
    UNIQUE (tabphyname, colphyname),
    UNIQUE (tabphyname, collogname),
    UNIQUE (tabphyname, collabel),
    FOREIGN KEY (tabphyname) REFERENCES {schema}.mdstattabs(tabphyname) ON DELETE CASCADE,
    FOREIGN KEY (domainname) REFERENCES {schema}.mdstatdommas(domainname) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.sdvfolderattribute (
    folderkey INTEGER NOT NULL REFERENCES {schema}.sdvfolder(folderkey) ON DELETE CASCADE,
    attributekey INTEGER NOT NULL REFERENCES {schema}.sdvattribute(attributekey) ON DELETE CASCADE,
    PRIMARY KEY (folderkey, attributekey));
    
------- LEVEL 1.1 -------
CREATE TABLE IF NOT EXISTS {schema}.sapolygon (
    areasymbol VARCHAR (20),
    spatialver DOUBLE PRECISION,
    lkey VARCHAR (30) PRIMARY KEY,
    area_ha DOUBLE PRECISION,
    FOREIGN KEY (lkey) REFERENCES {schema}.legend(lkey) ON DELETE CASCADE);
    
CREATE TABLE IF NOT EXISTS {schema}.distlegendmd (
    areatypename VARCHAR (45),
    areasymbol VARCHAR (20),
    areaname VARCHAR (135),
    ssastatus VARCHAR (254),
    cordate DATE,
    exportcertstatus VARCHAR (254),
    exportcertdate DATE,
    exportmetadata TEXT,
    lkey VARCHAR (30) NOT NULL,
    distmdkey VARCHAR (30) UNIQUE NOT NULL,
    distlegendmdkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (lkey) REFERENCES {schema}.legend(lkey) ON DELETE CASCADE);

  
------- LEVEL 2 -------
CREATE TABLE IF NOT EXISTS {schema}.distmd (
    distgendate DATE,
    diststatus VARCHAR (254) NOT NULL,
    interpmaxreasons SMALLINT,
    distmdkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (distmdkey) REFERENCES {schema}.distlegendmd(distmdkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.distinterpmd (
    rulename VARCHAR (60),
    ruledesign VARCHAR (254) NOT NULL,
    ruledesc TEXT,
    dataafuse VARCHAR (3),
    mrecentrulecwlu DATE,
    rulekey VARCHAR (30) NOT NULL,
    distmdkey VARCHAR (30) NOT NULL,
    distinterpmdkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (distmdkey) REFERENCES {schema}.distlegendmd(distmdkey) ON DELETE CASCADE);
    
CREATE TABLE IF NOT EXISTS {schema}.laoverlap (
    areatypename VARCHAR (45) NOT NULL,
    areasymbol VARCHAR (20) NOT NULL,
    areaname VARCHAR (135),
    areaovacres INTEGER,
    lkey VARCHAR (30) NOT NULL,
    lareaovkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (lkey) REFERENCES {schema}.legend(lkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.legendtext (
    recdate DATE,
    legendtextkind VARCHAR (254),
    textcat VARCHAR (20),
    textsubcat VARCHAR (20),
    text TEXT,
    lkey VARCHAR (30) NOT NULL,
    legtextkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (lkey) REFERENCES {schema}.legend(lkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.mapunit (
    musym VARCHAR (6) NOT NULL,
    muname VARCHAR (175),
    mukind VARCHAR (254),
    mustatus VARCHAR (254),
    muacres INTEGER,
    mapunitlfw_l SMALLINT,
    mapunitlfw_r SMALLINT,
    mapunitlfw_h SMALLINT,
    mapunitpfa_l DOUBLE PRECISION,
    mapunitpfa_r DOUBLE PRECISION,
    mapunitpfa_h DOUBLE PRECISION,
    farmlndcl VARCHAR (254),
    muhelcl VARCHAR (254),
    muwathelcl VARCHAR (254),
    muwndhelcl VARCHAR (254),
    interpfocus VARCHAR (30),
    invesintens VARCHAR (254),
    iacornsr SMALLINT,
    nhiforsoigrp VARCHAR (254),
    nhspiagr DOUBLE PRECISION,
    vtsepticsyscl VARCHAR (254),
    mucertstat VARCHAR (254),
    lkey VARCHAR (30) NOT NULL,
    mukey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (lkey) REFERENCES {schema}.legend(lkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.mdstatidxdet (
    tabphyname VARCHAR (30) NOT NULL,
    idxphyname VARCHAR (30) NOT NULL,
    idxcolsequence SMALLINT NOT NULL,
    colphyname VARCHAR (30) NOT NULL,
    PRIMARY KEY (tabphyname, idxphyname, idxcolsequence),
    UNIQUE (tabphyname, idxphyname, colphyname),
    FOREIGN KEY (tabphyname, idxphyname) REFERENCES {schema}.mdstatidxmas(tabphyname, idxphyname) ON DELETE CASCADE);
    
CREATE TABLE IF NOT EXISTS {schema}.mdstatrshipdet (
    ltabphyname VARCHAR (30) NOT NULL,
    rtabphyname VARCHAR (30) NOT NULL,
    relationshipname VARCHAR (30) NOT NULL,
    ltabcolphyname VARCHAR (30) NOT NULL,
    rtabcolphyname VARCHAR (30) NOT NULL,
    PRIMARY KEY (ltabphyname, rtabphyname, relationshipname, ltabcolphyname, rtabcolphyname),
    FOREIGN KEY (ltabphyname, rtabphyname, relationshipname) REFERENCES {schema}.mdstatrshipmas(ltabphyname, rtabphyname, relationshipname)
    ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.sainterp (
    areasymbol VARCHAR (20) NOT NULL,
    interpname VARCHAR (60) NOT NULL,
    interptype VARCHAR (254) NOT NULL,
    interpdesc TEXT,
    interpdesigndate DATE NOT NULL,
    interpgendate DATE NOT NULL,
    interpmaxreasons SMALLINT,
    sacatalogkey VARCHAR (30) NOT NULL,
    sainterpkey VARCHAR (30) PRIMARY KEY,
    UNIQUE (areasymbol, interpname),
    FOREIGN KEY (sacatalogkey) REFERENCES {schema}.sacatalog(sacatalogkey) ON DELETE CASCADE);

------- LEVEL 3 -------
CREATE TABLE IF NOT EXISTS {schema}.component (
    comppct_l SMALLINT,
    comppct_r SMALLINT,
    comppct_h SMALLINT,
    compname VARCHAR (60),
    compkind VARCHAR (254),
    majcompflag VARCHAR (3),
    otherph VARCHAR (40),
    localphase VARCHAR (40),
    slope_l DOUBLE PRECISION,
    slope_r DOUBLE PRECISION,
    slope_h DOUBLE PRECISION,
    slopelenusle_l SMALLINT,
    slopelenusle_r SMALLINT,
    slopelenusle_h SMALLINT,
    runoff VARCHAR (254),
    tfact SMALLINT,
    wei VARCHAR (254),
    weg VARCHAR (254),
    erocl VARCHAR (254),
    earthcovkind1 VARCHAR (254),
    earthcovkind2 VARCHAR (254),
    hydricon VARCHAR (254),
    hydricrating VARCHAR (254),
    drainagecl VARCHAR (254),
    elev_l DOUBLE PRECISION,
    elev_r DOUBLE PRECISION,
    elev_h DOUBLE PRECISION,
    aspectccwise SMALLINT,
    aspectrep SMALLINT,
    aspectcwise SMALLINT,
    geomdesc TEXT,
    albedodry_l DOUBLE PRECISION,
    albedodry_r DOUBLE PRECISION,
    albedodry_h DOUBLE PRECISION,
    airtempa_l DOUBLE PRECISION,
    airtempa_r DOUBLE PRECISION,
    airtempa_h DOUBLE PRECISION,
    map_l SMALLINT,
    map_r SMALLINT,
    map_h SMALLINT,
    reannualprecip_l SMALLINT,
    reannualprecip_r SMALLINT,
    reannualprecip_h SMALLINT,
    ffd_l SMALLINT,
    ffd_r SMALLINT,
    ffd_h SMALLINT,
    nirrcapcl VARCHAR (254),
    nirrcapscl VARCHAR (254),
    nirrcapunit SMALLINT,
    irrcapcl VARCHAR (254),
    irrcapscl VARCHAR (254),
    irrcapunit SMALLINT,
    cropprodindex SMALLINT,
    constreeshrubgrp VARCHAR (254),
    wndbrksuitgrp VARCHAR (254),
    rsprod_l INTEGER,
    rsprod_r INTEGER,
    rsprod_h INTEGER,
    foragesuitgrpid VARCHAR (11),
    wlgrain VARCHAR (254),
    wlgrass VARCHAR (254),
    wlherbaceous VARCHAR (254),
    wlshrub VARCHAR (254),
    wlconiferous VARCHAR (254),
    wlhardwood VARCHAR (254),
    wlwetplant VARCHAR (254),
    wlshallowwat VARCHAR (254),
    wlrangeland VARCHAR (254),
    wlopenland VARCHAR (254),
    wlwoodland VARCHAR (254),
    wlwetland VARCHAR (254),
    soilslippot VARCHAR (254),
    frostact VARCHAR (254),
    initsub_l SMALLINT,
    initsub_r SMALLINT,
    initsub_h SMALLINT,
    totalsub_l SMALLINT,
    totalsub_r SMALLINT,
    totalsub_h SMALLINT,
    hydgrp VARCHAR (254),
    corcon VARCHAR (254),
    corsteel VARCHAR (254),
    taxclname VARCHAR (120),
    taxorder VARCHAR (254),
    taxsuborder VARCHAR (254),
    taxgrtgroup VARCHAR (254),
    taxsubgrp VARCHAR (254),
    taxpartsize VARCHAR (254),
    taxpartsizemod VARCHAR (254),
    taxceactcl VARCHAR (254),
    taxreaction VARCHAR (254),
    taxtempcl VARCHAR (254),
    taxmoistscl VARCHAR (254),
    taxtempregime VARCHAR (254),
    soiltaxedition VARCHAR (254),
    castorieindex SMALLINT,
    flecolcomnum VARCHAR (5),
    flhe VARCHAR (3),
    flphe VARCHAR (3),
    flsoilleachpot VARCHAR (254),
    flsoirunoffpot VARCHAR (254),
    fltemik2use VARCHAR (3),
    fltriumph2use VARCHAR (3),
    indraingrp VARCHAR (3),
    innitrateleachi SMALLINT,
    misoimgmtgrp VARCHAR (254),
    vasoimgtgrp VARCHAR (254),
    mukey VARCHAR (30) NOT NULL,
    cokey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (mukey) REFERENCES {schema}.mapunit(mukey) ON DELETE CASCADE);
    
CREATE TABLE IF NOT EXISTS {schema}.muaggatt (
    musym VARCHAR (6) NOT NULL,
    muname VARCHAR (175),
    mustatus VARCHAR (254),
    slopegraddcp DOUBLE PRECISION,
    slopegradwta DOUBLE PRECISION,
    brockdepmin SMALLINT,
    wtdepannmin SMALLINT,
    wtdepaprjunmin SMALLINT,
    flodfreqdcd VARCHAR (254),
    flodfreqmax VARCHAR (254),
    pondfreqprs VARCHAR (254),
    aws025wta DOUBLE PRECISION,
    aws050wta DOUBLE PRECISION,
    aws0100wta DOUBLE PRECISION,
    aws0150wta DOUBLE PRECISION,
    drclassdcd VARCHAR (254),
    drclasswettest VARCHAR (254),
    hydgrpdcd VARCHAR (254),
    iccdcd VARCHAR (254),
    iccdcdpct SMALLINT,
    niccdcd VARCHAR (254),
    niccdcdpct SMALLINT,
    engdwobdcd VARCHAR (254),
    engdwbdcd VARCHAR (254),
    engdwbll VARCHAR (254),
    engdwbml VARCHAR (254),
    engstafdcd VARCHAR (254),
    engstafll VARCHAR (254),
    engstafml VARCHAR (254),
    engsldcd VARCHAR (254),
    engsldcp VARCHAR (254),
    englrsdcd VARCHAR (254),
    engcmssdcd VARCHAR (254),
    engcmssmp VARCHAR (254),
    urbrecptdcd VARCHAR (254),
    urbrecptwta DOUBLE PRECISION,
    forpehrtdcp VARCHAR (254),
    hydclprs VARCHAR (254),
    awmmfpwwta DOUBLE PRECISION,
    mukey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (mukey) REFERENCES {schema}.mapunit(mukey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.muaoverlap (
    areaovacres INTEGER,
    lareaovkey VARCHAR (30) NOT NULL,
    mukey VARCHAR (30) NOT NULL,
    muareaovkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (mukey) REFERENCES {schema}.mapunit(mukey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.mucropyld (
    cropname VARCHAR (254),
    yldunits VARCHAR (254),
    nonirryield_l DOUBLE PRECISION,
    nonirryield_r DOUBLE PRECISION,
    nonirryield_h DOUBLE PRECISION,
    irryield_l DOUBLE PRECISION,
    irryield_r DOUBLE PRECISION,
    irryield_h DOUBLE PRECISION,
    mukey VARCHAR (30) NOT NULL,
    mucrpyldkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (mukey) REFERENCES {schema}.mapunit(mukey) ON DELETE CASCADE);
 
CREATE TABLE IF NOT EXISTS {schema}.muline (
    areasymbol VARCHAR (20),
    spatialver DOUBLE PRECISION,
    musym VARCHAR (6),
    mukey VARCHAR (30) PRIMARY KEY,
    length_m DOUBLE PRECISION,
    FOREIGN KEY (mukey) REFERENCES {schema}.mapunit(mukey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.mupoint (
    areasymbol VARCHAR (20),
    spatialver DOUBLE PRECISION,
    musym VARCHAR (6),
    mukey VARCHAR (30) PRIMARY KEY,
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,
    FOREIGN KEY (mukey) REFERENCES {schema}.mapunit(mukey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.mupolygon (
    areasymbol VARCHAR (20),
    spatialver DOUBLE PRECISION,
    musym VARCHAR (6),
    mukey VARCHAR (30) PRIMARY KEY,
    area_ha DOUBLE PRECISION,
    FOREIGN KEY (mukey) REFERENCES {schema}.mapunit(mukey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.mutext (
    recdate DATE,
    mapunittextkind VARCHAR (254),
    textcat VARCHAR (20),
    textsubcat VARCHAR (20),
    text TEXT,
    mukey VARCHAR (30) NOT NULL,
    mutextkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (mukey) REFERENCES {schema}.mapunit(mukey) ON DELETE CASCADE);

------- LEVEL 4 -------
CREATE TABLE IF NOT EXISTS {schema}.chorizon (
    hzname VARCHAR (12),
    desgndisc SMALLINT,
    desgnmaster VARCHAR (254),
    desgnmasterprime VARCHAR (254),
    desgnvert SMALLINT,
    hzdept_l SMALLINT,
    hzdept_r SMALLINT,
    hzdept_h SMALLINT,
    hzdepb_l SMALLINT,
    hzdepb_r SMALLINT,
    hzdepb_h SMALLINT,
    hzthk_l SMALLINT,
    hzthk_r SMALLINT,
    hzthk_h SMALLINT,
    fraggt10_l SMALLINT,
    fraggt10_r SMALLINT,
    fraggt10_h SMALLINT,
    frag3to10_l SMALLINT,
    frag3to10_r SMALLINT,
    frag3to10_h SMALLINT,
    sieveno4_l DOUBLE PRECISION,
    sieveno4_r DOUBLE PRECISION,
    sieveno4_h DOUBLE PRECISION,
    sieveno10_l DOUBLE PRECISION,
    sieveno10_r DOUBLE PRECISION,
    sieveno10_h DOUBLE PRECISION,
    sieveno40_l DOUBLE PRECISION,
    sieveno40_r DOUBLE PRECISION,
    sieveno40_h DOUBLE PRECISION,
    sieveno200_l DOUBLE PRECISION,
    sieveno200_r DOUBLE PRECISION,
    sieveno200_h DOUBLE PRECISION,
    sandtotal_l DOUBLE PRECISION,
    sandtotal_r DOUBLE PRECISION,
    sandtotal_h DOUBLE PRECISION,
    sandvc_l DOUBLE PRECISION,
    sandvc_r DOUBLE PRECISION,
    sandvc_h DOUBLE PRECISION,
    sandco_l DOUBLE PRECISION,
    sandco_r DOUBLE PRECISION,
    sandco_h DOUBLE PRECISION,
    sandmed_l DOUBLE PRECISION,
    sandmed_r DOUBLE PRECISION,
    sandmed_h DOUBLE PRECISION,
    sandfine_l DOUBLE PRECISION,
    sandfine_r DOUBLE PRECISION,
    sandfine_h DOUBLE PRECISION,
    sandvf_l DOUBLE PRECISION,
    sandvf_r DOUBLE PRECISION,
    sandvf_h DOUBLE PRECISION,
    silttotal_l DOUBLE PRECISION,
    silttotal_r DOUBLE PRECISION,
    silttotal_h DOUBLE PRECISION,
    siltco_l DOUBLE PRECISION,
    siltco_r DOUBLE PRECISION,
    siltco_h DOUBLE PRECISION,
    siltfine_l DOUBLE PRECISION,
    siltfine_r DOUBLE PRECISION,
    siltfine_h DOUBLE PRECISION,
    claytotal_l DOUBLE PRECISION,
    claytotal_r DOUBLE PRECISION,
    claytotal_h DOUBLE PRECISION,
    claysizedcarb_l DOUBLE PRECISION,
    claysizedcarb_r DOUBLE PRECISION,
    claysizedcarb_h DOUBLE PRECISION,
    om_l DOUBLE PRECISION,
    om_r DOUBLE PRECISION,
    om_h DOUBLE PRECISION,
    dbtenthbar_l DOUBLE PRECISION,
    dbtenthbar_r DOUBLE PRECISION,
    dbtenthbar_h DOUBLE PRECISION,
    dbthirdbar_l DOUBLE PRECISION,
    dbthirdbar_r DOUBLE PRECISION,
    dbthirdbar_h DOUBLE PRECISION,
    dbfifteenbar_l DOUBLE PRECISION,
    dbfifteenbar_r DOUBLE PRECISION,
    dbfifteenbar_h DOUBLE PRECISION,
    dbovendry_l DOUBLE PRECISION,
    dbovendry_r DOUBLE PRECISION,
    dbovendry_h DOUBLE PRECISION,
    partdensity DOUBLE PRECISION,
    ksat_l DOUBLE PRECISION,
    ksat_r DOUBLE PRECISION,
    ksat_h DOUBLE PRECISION,
    awc_l DOUBLE PRECISION,
    awc_r DOUBLE PRECISION,
    awc_h DOUBLE PRECISION,
    wtenthbar_l DOUBLE PRECISION,
    wtenthbar_r DOUBLE PRECISION,
    wtenthbar_h DOUBLE PRECISION,
    wthirdbar_l DOUBLE PRECISION,
    wthirdbar_r DOUBLE PRECISION,
    wthirdbar_h DOUBLE PRECISION,
    wfifteenbar_l DOUBLE PRECISION,
    wfifteenbar_r DOUBLE PRECISION,
    wfifteenbar_h DOUBLE PRECISION,
    wsatiated_l SMALLINT,
    wsatiated_r SMALLINT,
    wsatiated_h SMALLINT,
    lep_l DOUBLE PRECISION,
    lep_r DOUBLE PRECISION,
    lep_h DOUBLE PRECISION,
    ll_l DOUBLE PRECISION,
    ll_r DOUBLE PRECISION,
    ll_h DOUBLE PRECISION,
    pi_l DOUBLE PRECISION,
    pi_r DOUBLE PRECISION,
    pi_h DOUBLE PRECISION,
    aashind_l SMALLINT,
    aashind_r SMALLINT,
    aashind_h SMALLINT,
    kwfact VARCHAR (254),
    kffact VARCHAR (254),
    caco3_l SMALLINT,
    caco3_r SMALLINT,
    caco3_h SMALLINT,
    gypsum_l SMALLINT,
    gypsum_r SMALLINT,
    gypsum_h SMALLINT,
    sar_l DOUBLE PRECISION,
    sar_r DOUBLE PRECISION,
    sar_h DOUBLE PRECISION,
    ec_l DOUBLE PRECISION,
    ec_r DOUBLE PRECISION,
    ec_h DOUBLE PRECISION,
    cec7_l DOUBLE PRECISION,
    cec7_r DOUBLE PRECISION,
    cec7_h DOUBLE PRECISION,
    ecec_l DOUBLE PRECISION,
    ecec_r DOUBLE PRECISION,
    ecec_h DOUBLE PRECISION,
    sumbases_l DOUBLE PRECISION,
    sumbases_r DOUBLE PRECISION,
    sumbases_h DOUBLE PRECISION,
    ph1to1h2o_l DOUBLE PRECISION,
    ph1to1h2o_r DOUBLE PRECISION,
    ph1to1h2o_h DOUBLE PRECISION,
    ph01mcacl2_l DOUBLE PRECISION,
    ph01mcacl2_r DOUBLE PRECISION,
    ph01mcacl2_h DOUBLE PRECISION,
    freeiron_l DOUBLE PRECISION,
    freeiron_r DOUBLE PRECISION,
    freeiron_h DOUBLE PRECISION,
    feoxalate_l DOUBLE PRECISION,
    feoxalate_r DOUBLE PRECISION,
    feoxalate_h DOUBLE PRECISION,
    extracid_l DOUBLE PRECISION,
    extracid_r DOUBLE PRECISION,
    extracid_h DOUBLE PRECISION,
    extral_l DOUBLE PRECISION,
    extral_r DOUBLE PRECISION,
    extral_h DOUBLE PRECISION,
    aloxalate_l DOUBLE PRECISION,
    aloxalate_r DOUBLE PRECISION,
    aloxalate_h DOUBLE PRECISION,
    pbray1_l DOUBLE PRECISION,
    pbray1_r DOUBLE PRECISION,
    pbray1_h DOUBLE PRECISION,
    poxalate_l DOUBLE PRECISION,
    poxalate_r DOUBLE PRECISION,
    poxalate_h DOUBLE PRECISION,
    ph2osoluble_l DOUBLE PRECISION,
    ph2osoluble_r DOUBLE PRECISION,
    ph2osoluble_h DOUBLE PRECISION,
    ptotal_l DOUBLE PRECISION,
    ptotal_r DOUBLE PRECISION,
    ptotal_h DOUBLE PRECISION,
    excavdifcl VARCHAR (254),
    excavdifms VARCHAR (254),
    cokey VARCHAR (30) NOT NULL,
    chkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cocanopycover (
    plantcov SMALLINT,
    plantsym VARCHAR (8) NOT NULL,
    plantsciname VARCHAR (127),
    plantcomname VARCHAR (60),
    cokey VARCHAR (30) NOT NULL,
    cocanopycovkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cocropyld (
    cropname VARCHAR (254),
    yldunits VARCHAR (254),
    nonirryield_l DOUBLE PRECISION,
    nonirryield_r DOUBLE PRECISION,
    nonirryield_h DOUBLE PRECISION,
    irryield_l DOUBLE PRECISION,
    irryield_r DOUBLE PRECISION,
    irryield_h DOUBLE PRECISION,
    cropprodindex SMALLINT,
    vasoiprdgrp VARCHAR (254),
    cokey VARCHAR (30) NOT NULL,
    cocropyldkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.codiagfeatures (
    featkind VARCHAR (254),
    featdept_l SMALLINT,
    featdept_r SMALLINT,
    featdept_h SMALLINT,
    featdepb_l SMALLINT,
    featdepb_r SMALLINT,
    featdepb_h SMALLINT,
    featthick_l SMALLINT,
    featthick_r SMALLINT,
    featthick_h SMALLINT,
    cokey VARCHAR (30) NOT NULL,
    codiagfeatkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.coecoclass (
    ecoclasstypename VARCHAR (60) NOT NULL,
    ecoclassref VARCHAR (254),
    ecoclassid VARCHAR (30) NOT NULL,
    ecoclassname TEXT,
    cokey VARCHAR (30) NOT NULL,
    coecoclasskey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.coeplants (
    plantsym VARCHAR (8) NOT NULL,
    plantsciname VARCHAR (127),
    plantcomname VARCHAR (60),
    forestunprod SMALLINT,
    rangeprod SMALLINT,
    cokey VARCHAR (30) NOT NULL,
    coeplantskey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.coerosionacc (
    erokind VARCHAR (254),
    rvindicator VARCHAR (3) NOT NULL,
    cokey VARCHAR (30) NOT NULL,
    coeroacckey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.coforprod (
    plantsym VARCHAR (8) NOT NULL,
    plantsciname VARCHAR (127),
    plantcomname VARCHAR (60),
    siteindexbase VARCHAR (254),
    siteindex_l SMALLINT,
    siteindex_r SMALLINT,
    siteindex_h SMALLINT,
    fprod_l DOUBLE PRECISION,
    fprod_r DOUBLE PRECISION,
    fprod_h DOUBLE PRECISION,
    cokey VARCHAR (30) NOT NULL,
    cofprodkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cogeomordesc (
    geomftname VARCHAR (30) NOT NULL,
    geomfname VARCHAR (50) NOT NULL,
    geomfmod VARCHAR (60),
    geomfeatid SMALLINT,
    existsonfeat SMALLINT,
    rvindicator VARCHAR (3) NOT NULL,
    cokey VARCHAR (30) NOT NULL,
    cogeomdkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cohydriccriteria (
    hydriccriterion VARCHAR (254),
    cokey VARCHAR (30) NOT NULL,
    cohydcritkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cointerp (
    cokey VARCHAR (30) NOT NULL,
    mrulekey VARCHAR (30) NOT NULL,
    mrulename VARCHAR (60) NOT NULL,
    seqnum SMALLINT NOT NULL,
    rulekey VARCHAR (30) NOT NULL,
    rulename VARCHAR (60) NOT NULL,
    ruledepth SMALLINT NOT NULL,
    interpll DOUBLE PRECISION,
    interpllc VARCHAR (254),
    interplr DOUBLE PRECISION,
    interplrc VARCHAR (254),
    interphr DOUBLE PRECISION,
    interphrc VARCHAR (254),
    interphh DOUBLE PRECISION,
    interphhc VARCHAR (254),
    nullpropdatabool VARCHAR (3),
    defpropdatabool VARCHAR (3),
    incpropdatabool VARCHAR (3),
    cointerpkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.comonth (
    monthseq SMALLINT,
    month VARCHAR (254),
    flodfreqcl VARCHAR (254),
    floddurcl VARCHAR (254),
    pondfreqcl VARCHAR (254),
    ponddurcl VARCHAR (254),
    ponddep_l SMALLINT,
    ponddep_r SMALLINT,
    ponddep_h SMALLINT,
    dlyavgprecip_l SMALLINT,
    dlyavgprecip_r SMALLINT,
    dlyavgprecip_h SMALLINT,
    dlyavgpotet_l SMALLINT,
    dlyavgpotet_r SMALLINT,
    dlyavgpotet_h SMALLINT,
    cokey VARCHAR (30) NOT NULL,
    comonthkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.copmgrp (
    pmgroupname VARCHAR (252),
    rvindicator VARCHAR (3) NOT NULL,
    cokey VARCHAR (30) NOT NULL,
    copmgrpkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.copwindbreak (
    wndbrkht_l DOUBLE PRECISION,
    wndbrkht_r DOUBLE PRECISION,
    wndbrkht_h DOUBLE PRECISION,
    plantsym VARCHAR (8) NOT NULL,
    plantsciname VARCHAR (127),
    plantcomname VARCHAR (60),
    cokey VARCHAR (30) NOT NULL,
    copwindbreakkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.corestrictions (
    reskind VARCHAR (254),
    reshard VARCHAR (254),
    resdept_l SMALLINT,
    resdept_r SMALLINT,
    resdept_h SMALLINT,
    resdepb_l SMALLINT,
    resdepb_r SMALLINT,
    resdepb_h SMALLINT,
    resthk_l SMALLINT,
    resthk_r SMALLINT,
    resthk_h SMALLINT,
    cokey VARCHAR (30) NOT NULL,
    corestrictkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cosurffrags (
    sfragcov_l DOUBLE PRECISION,
    sfragcov_r DOUBLE PRECISION,
    sfragcov_h DOUBLE PRECISION,
    distrocks_l DOUBLE PRECISION,
    distrocks_r DOUBLE PRECISION,
    distrocks_h DOUBLE PRECISION,
    sfragkind VARCHAR (254),
    sfragsize_l SMALLINT,
    sfragsize_r SMALLINT,
    sfragsize_h SMALLINT,
    sfragshp VARCHAR (254),
    sfraground VARCHAR (254),
    sfraghard VARCHAR (254),
    cokey VARCHAR (30) NOT NULL,
    cosurffragskey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cotaxfmmin (
    taxminalogy VARCHAR (254),
    cokey VARCHAR (30) NOT NULL,
    cotaxfmminkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cotaxmoistcl (
    taxmoistcl VARCHAR (254),
    cokey VARCHAR (30) NOT NULL,
    cotaxmckey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cotext (
    recdate DATE,
    comptextkind VARCHAR (254),
    textcat VARCHAR (20),
    textsubcat VARCHAR (20),
    text TEXT,
    cokey VARCHAR (30) NOT NULL,
    cotextkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cotreestomng (
    plantsym VARCHAR (8) NOT NULL,
    plantsciname VARCHAR (127),
    plantcomname VARCHAR (60),
    cokey VARCHAR (30) NOT NULL,
    cotreestomngkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cotxfmother (
    taxfamother VARCHAR (254),
    cokey VARCHAR (30) NOT NULL,
    cotaxfokey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cokey) REFERENCES {schema}.component(cokey) ON DELETE CASCADE);

------- LEVEL 5 -------
CREATE TABLE IF NOT EXISTS {schema}.chaashto (
    aashtocl VARCHAR (254),
    rvindicator VARCHAR (3) NOT NULL,
    chkey VARCHAR (30) NOT NULL,
    chaashtokey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (chkey) REFERENCES {schema}.chorizon(chkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.chconsistence (
    rupresblkmst VARCHAR (254),
    rupresblkdry VARCHAR (254),
    rupresblkcem VARCHAR (254),
    rupresplate VARCHAR (254),
    mannerfailure VARCHAR (254),
    stickiness VARCHAR (254),
    plasticity VARCHAR (254),
    rvindicator VARCHAR (3) NOT NULL,
    chkey VARCHAR (30) NOT NULL,
    chconsistkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (chkey) REFERENCES {schema}.chorizon(chkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.chdesgnsuffix (
    desgnsuffix VARCHAR (254),
    chkey VARCHAR (30) NOT NULL,
    chdesgnsfxkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (chkey) REFERENCES {schema}.chorizon(chkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.chfrags (
    fragvol_l SMALLINT,
    fragvol_r SMALLINT,
    fragvol_h SMALLINT,
    fragkind VARCHAR (254),
    fragsize_l SMALLINT,
    fragsize_r SMALLINT,
    fragsize_h SMALLINT,
    fragshp VARCHAR (254),
    fraground VARCHAR (254),
    fraghard VARCHAR (254),
    chkey VARCHAR (30) NOT NULL,
    chfragskey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (chkey) REFERENCES {schema}.chorizon(chkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.chpores (
    poreqty_l DOUBLE PRECISION,
    poreqty_r DOUBLE PRECISION,
    poreqty_h DOUBLE PRECISION,
    poresize VARCHAR (254),
    porecont VARCHAR (254),
    poreshp VARCHAR (254),
    rvindicator VARCHAR (3) NOT NULL,
    chkey VARCHAR (30) NOT NULL,
    chporeskey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (chkey) REFERENCES {schema}.chorizon(chkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.chstructgrp (
    structgrpname VARCHAR (254),
    rvindicator VARCHAR (3) NOT NULL,
    chkey VARCHAR (30) NOT NULL,
    chstructgrpkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (chkey) REFERENCES {schema}.chorizon(chkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.chtext (
    recdate DATE,
    chorizontextkind VARCHAR (254),
    textcat VARCHAR (20),
    textsubcat VARCHAR (20),
    text TEXT,
    chkey VARCHAR (30) NOT NULL,
    chtextkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (chkey) REFERENCES {schema}.chorizon(chkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.chtexturegrp (
    texture VARCHAR (30),
    stratextsflag VARCHAR (3) NOT NULL,
    rvindicator VARCHAR (3) NOT NULL,
    texdesc TEXT,
    chkey VARCHAR (30) NOT NULL,
    chtgkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (chkey) REFERENCES {schema}.chorizon(chkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.chunified (
    unifiedcl VARCHAR (254),
    rvindicator VARCHAR (3) NOT NULL,
    chkey VARCHAR (30) NOT NULL,
    chunifiedkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (chkey) REFERENCES {schema}.chorizon(chkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.coforprodo (
    siteindexbase VARCHAR (254),
    siteindex_l SMALLINT,
    siteindex_r SMALLINT,
    siteindex_h SMALLINT,
    fprod_l DOUBLE PRECISION,
    fprod_r DOUBLE PRECISION,
    fprod_h DOUBLE PRECISION,
    fprodunits VARCHAR (254),
    cofprodkey VARCHAR (30) NOT NULL,
    cofprodokey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cofprodkey) REFERENCES {schema}.coforprod(cofprodkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.copm (
    pmorder SMALLINT,
    pmmodifier VARCHAR (254),
    pmgenmod VARCHAR (60),
    pmkind VARCHAR (254),
    pmorigin VARCHAR (254),
    copmgrpkey VARCHAR (30) NOT NULL,
    copmkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (copmgrpkey) REFERENCES {schema}.copmgrp(copmgrpkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cosoilmoist (
    soimoistdept_l SMALLINT,
    soimoistdept_r SMALLINT,
    soimoistdept_h SMALLINT,
    soimoistdepb_l SMALLINT,
    soimoistdepb_r SMALLINT,
    soimoistdepb_h SMALLINT,
    soimoiststat VARCHAR (254),
    comonthkey VARCHAR (30) NOT NULL,
    cosoilmoistkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (comonthkey) REFERENCES {schema}.comonth(comonthkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cosoiltemp (
    soitempmm SMALLINT,
    soitempdept_l SMALLINT,
    soitempdept_r SMALLINT,
    soitempdept_h SMALLINT,
    soitempdepb_l SMALLINT,
    soitempdepb_r SMALLINT,
    soitempdepb_h SMALLINT,
    comonthkey VARCHAR (30) NOT NULL,
    cosoiltempkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (comonthkey) REFERENCES {schema}.comonth(comonthkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cosurfmorphgc (
    geomposmntn VARCHAR (254),
    geomposhill VARCHAR (254),
    geompostrce VARCHAR (254),
    geomposflats VARCHAR (254),
    cogeomdkey VARCHAR (30) NOT NULL,
    cosurfmorgckey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cogeomdkey) REFERENCES {schema}.cogeomordesc(cogeomdkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cosurfmorphhpp (
    hillslopeprof VARCHAR (254),
    cogeomdkey VARCHAR (30) NOT NULL,
    cosurfmorhppkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cogeomdkey) REFERENCES {schema}.cogeomordesc(cogeomdkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cosurfmorphmr (
    geomicrorelief VARCHAR (254),
    cogeomdkey VARCHAR (30) NOT NULL,
    cosurfmormrkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cogeomdkey) REFERENCES {schema}.cogeomordesc(cogeomdkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.cosurfmorphss (
    geomacross VARCHAR (254),
    geomdown VARCHAR (254),
    cogeomdkey VARCHAR (30) NOT NULL,
    cosurfmorsskey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (cogeomdkey) REFERENCES {schema}.cogeomordesc(cogeomdkey) ON DELETE CASCADE);

------- LEVEL 6 -------
CREATE TABLE IF NOT EXISTS {schema}.chstruct (
    structgrade VARCHAR (254),
    structsize VARCHAR (254),
    structtype VARCHAR (254),
    structid SMALLINT,
    structpartsto SMALLINT,
    chstructgrpkey VARCHAR (30) NOT NULL,
    chstructkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (chstructgrpkey) REFERENCES {schema}.chstructgrp(chstructgrpkey) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS {schema}.chtexture (
    texcl VARCHAR (254),
    lieutex VARCHAR (254),
    chtgkey VARCHAR (30) NOT NULL,
    chtkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (chtgkey) REFERENCES {schema}.chtexturegrp(chtgkey) ON DELETE CASCADE);

------- LEVEL 7 -------

CREATE TABLE IF NOT EXISTS {schema}.chtexturemod (
    texmod VARCHAR (254),
    chtkey VARCHAR (30) NOT NULL,
    chtexmodkey VARCHAR (30) PRIMARY KEY,
    FOREIGN KEY (chtkey) REFERENCES {schema}.chtexture(chtkey) ON DELETE CASCADE);
    
------- Geometry ------
ALTER TABLE {schema}.featline ADD COLUMN IF NOT EXISTS geom geometry(MULTILINESTRING, 4326);
ALTER TABLE {schema}.featpoint ADD COLUMN IF NOT EXISTS geom geometry(MULTIPOINT, 4326);
ALTER TABLE {schema}.muline ADD COLUMN IF NOT EXISTS geom geometry(MULTILINESTRING, 4326);
ALTER TABLE {schema}.mupoint ADD COLUMN IF NOT EXISTS geom geometry(MULTIPOINT, 4326);
ALTER TABLE {schema}.mupolygon ADD COLUMN IF NOT EXISTS geom geometry(MULTIPOLYGON, 4326);
ALTER TABLE {schema}.sapolygon ADD COLUMN IF NOT EXISTS geom geometry(MULTIPOLYGON, 4326);
