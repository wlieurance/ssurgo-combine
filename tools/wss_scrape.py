#!/usr/bin/env python3
"Scrapes NRCS Web Soil Survey for soil data download links and then downloads them."

import argparse
import os
import zipfile
import time
from datetime import datetime
from urllib.parse import urlparse
import requests
import psycopg
from psycopg.rows import dict_row
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from io import StringIO
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
# internal
from import_soil import get_default_schema, DbConnect

def parse_versions(df):
    # one row per version record
    long_df = (
        df[["Area Symbol", "Version"]]
        .explode("Version", ignore_index=True)
    )
    # flatten each dict in Version into columns: data, date, version
    version_cols = pd.json_normalize(long_df["Version"])

    long_df = (
        long_df.drop(columns="Version")
        .join(version_cols)
        .query("data.notna()", engine="python")
    )
    long_df["date"] = pd.to_datetime(long_df["date"], format="%b %d, %Y")
    long_df["version"] = long_df["version"].astype("Int64")
   
    short_df = long_df.assign(
        data_short=lambda d: np.select(
            condlist=[
                d["data"].eq("Survey Area"),
                d["data"].eq("Tabular"),
                d["data"].eq("Spatial"),
            ],
            choicelist=["sa", "tab", "spat"],
            default="unk",
        )
    )

    df_pivot = short_df.pivot(
        index="Area Symbol",
        columns="data_short",
        values=["date", "version"],
    )

    df_pivot.columns = [f"{val}_{key}" for key, val in df_pivot.columns]
    df_pivot = df_pivot.reset_index()

    df_new = df.merge(df_pivot, how="left", on="Area Symbol")

    return df_new


# get db soil areas
def get_db_soil_areas(dbtype, schema, dbpath=None, user=None, dbname=None, 
                      port='5432', host='localhost', password=None):
    schema = get_default_schema(dbtype, schema)
    db = DbConnect(dbtype=dbtype, dbpath=dbpath, user=user, dbname=dbname,
                 port=port, host=host, pwd=password)
    db.open()
    c = db.conn.cursor()

    # conn = psycopg.connect(host='localhost', dbname='spatial', row_factory=dict_row)
    cat_sql = """
    SELECT areasymbol, areaname, saversion, saverest, tabularversion, tabularverest,
         tabnasisexportdate, tabcertstatus, sacatalogkey
    FROM soil.sacatalog;
    """
    c.execute(cat_sql)
    db_sc = c.fetchall()
    cols = [d[0] for d in c.description]
    sc_df = pd.DataFrame(db_sc, columns = cols)
    states = {x[0:2] for x in sc_df['areasymbol'].tolist()}
    db.close()

    return states, sc_df

def get_wss_areas(states):
    options = FirefoxOptions()
    options.headless = False  # optional: run without GUI

    driver = webdriver.Firefox(service=FirefoxService(executable_path="/usr/bin/geckodriver"),
                               options=options)

    # Load the page manually before running this, or automate to get there
    driver.get("https://websoilsurvey.nrcs.usda.gov/app/WebSoilSurvey.aspx")
    time.sleep(10)  # manually load AOI + generate report/download page if needed

    # should already be there but this gets us to start area
    driver.switch_to.default_content()

    # click on the Download Soils Data tab
    try:
        tab = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.ID, "Download_Soils_Data"))
        )
        tab.click()
        print("Clicked 'Download Soils Data' tab.")
        time.sleep(2)
    except Exception as e:
        print(f"Failed to click 'Download Soils Data' tab: {e}")

    # Click on the Download Soil Data for... Soil Survey Area (SSURGO) tab
    try:
        ssurgo_tab = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((
                By.ID, "Download_Soils_Data_for..._Soil_Survey_Area_.40.SSURGO.41._header"
                ))
            )
        ssurgo_tab.click()
        print("Expanded 'Soil Survey Area (SSURGO)' panel.")
        time.sleep(2)  # allow content to dynamically load
    except Exception as e:
        print(f"Failed to expand 'Soil Survey Area (SSURGO)' panel: {e}")

    # uncheck the option to include template database
    try:
        checkbox = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "soilsurveyareaincludetemplatedb"))
        )

        if checkbox.is_selected():
            checkbox.click()
            print("Unchecked 'Include Template DB' checkbox.")
            time.sleep(2)  # wait for the table to update
        else:
            print("Checkbox was already unchecked.")
    except Exception as e:
        print(f"Failed to uncheck checkbox: {e}")


    # switch our state to the appropriate choice
    wss = []
    for st in states:
        try:
            state_dropdown = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "state"))
            )
            select = Select(state_dropdown)
            select.select_by_value(st)

            print(f"Selected state: {st}")
            time.sleep(2)  # wait for the new table to load
        except Exception as e:
            print(f"Failed to select state from dropdown: {e}")

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        htable = soup.find("table", {'class': 'inputheader'})
        headers = [th.get_text(strip=True) for th in htable.find("thead").find_all("th")
                   if th.get_text(strip=True) != '']
        table = soup.find("table", {"id": "Soil_Survey_Area_.40.SSURGO.41._Download_Links_ancestor"})
        if not table:
            raise ValueError("Couldn't find the target table")
        #  df = pd.read_html(StringIO(str(table)))[0]
        #  df.columns = headers

        for row in table.find_all("tr"):
            d = {'state': st}
            cells = row.find_all("td")
            if not cells:
                continue
            for i, v in enumerate(cells):
                h = headers[i]
                if h == 'Version':
                    b = v.find_all("p", class_="datastatus")
                    lines = [p.get_text(separator="\n", strip=True).split("\n") for p in b]
                    text = []
                    for l in lines:
                        vd = {}
                        vd['data'], ver = l[0].strip(',').split(': ')
                        vd['date'] = l[1]
                        vd['version'] = ver.replace('Version ', '')
                        text.append(vd)
                elif h == 'Download Link':
                    link_tag = v.find("a")
                    if link_tag and link_tag.has_attr("href"):
                        link = link_tag["href"]
                        text = link
                else:
                    text = v.get_text(strip=True)
                d[h] = text
            wss.append(d)

    df = pd.DataFrame(wss)
    gr = df.groupby("Area Symbol", as_index=False).agg({
        "Name": "first",
        "state": lambda x: ','.join(x.dropna().astype(str)),
        "Data Availability": "first",
        "Version": "first",
        "Download Size": "first",
        "Download Link": "first"
    })
    driver.quit()
    
    return gr

############
# unfinished -- convert to functions
############
download_dir = "/home/wlieurance/Documents/temp/soil"
os.makedirs(download_dir, exist_ok=True)

# Apply to the Version column
df_new = parse_versions(gr)

# determine which soil areas we need to download
df_new.to_csv(os.path.join(download_dir, 'wss.csv'), index=False, date_format='%Y-%m-%d')
sc_join = sc_df[["areasymbol", "saversion", "saverest", "tabularversion", "tabularverest"]]
merged_df = pd.merge(df_new, sc_join, how='inner', left_on='Area Symbol', right_on='areasymbol')
db_sa = sc_join['areasymbol'].to_list()
matched_sa = merged_df['areasymbol'].to_list()
unmatched_sa = [x for x in db_sa if x not in matched_sa]
with open(os.path.join(download_dir, 'wss.log'), "w", encoding = 'utf8') as f:
    f.write("Unmatched areas:\n")
    for u in unmatched_sa:
        f.write(f'\t{u}\n')

filtered_df = merged_df.query("sa_version > saversion or tab_version > tabularversion")
remain_df = merged_df[~merged_df['Area Symbol'].isin(filtered_df['Area Symbol'])]
download_sa = filtered_df['areasymbol'].to_list()
with open(os.path.join(download_dir, 'wss.log'), "a", encoding = 'utf8') as f:
    f.write("Updated areas:\n")
    for u in download_sa:
        f.write(f'\t{u}\n')

skipped_sa = [x for x in matched_sa if x not in download_sa]
with open(os.path.join(download_dir, 'wss.log'), "a", encoding = 'utf8') as f:
    f.write("Not-updated areas:\n")
    for u in skipped_sa:
        f.write(f'\t{u}\n')

# --- Step 2: Download each ZIP ---
all_links = filtered_df['Download Link'].to_list()
downloaded = []
for link in all_links:
    try:
        filename = os.path.basename(urlparse(link).path)
        output_path = os.path.join(download_dir, filename)

        if os.path.exists(output_path):
            print(f"Already downloaded: {filename}")
            continue

        print(f"Downloading: {filename}")
        r = requests.get(link, timeout=15)
        r.raise_for_status()  # raise error on bad status codes
        with open(output_path, 'wb') as f:
            f.write(r.content)
        downloaded.append(output_path)

    except requests.exceptions.Timeout:
        print(f"Timeout while downloading: {link}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {link}: {e}")


for d in downloaded:
    print("Extracting", d)
    try:
        with zipfile.ZipFile(d, 'r') as zip_ref:
            zip_ref.extractall(download_dir)
        os.remove(d)
        print(f"Deleted: {d}")
    except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError) as e:
        print(f"Failed to extract {d}: {e}")


if __name__ == "__main__":
    my_args = sys.argv[1:]

    # parses script arguments
    parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            description='This script will scan a SSURGO database for imported soil areas and '
                        'attempt to download and unzip newer versions for use in the '
                        'import_soil.py script.')
    # positional arguments
    parser.add_argument('download_dir', help='path at which to store downloaded soil areas.')

    # optional arguments
    dbo = parser.add_argument_group('Database options')
    dbo_grp = dbo.add_mutually_exclusive_group(required=True)
    dbo_grp.add_argument('-D', '--dbpath',
                         help='File path for the SpatiaLite database to be used.')
    dbo_grp.add_argument('-d', '--dbname',
                          help='Name of the database in a PostreSQL instance to be used.')
    postgres = parser.add_argument_group('PostGIS options')
    postgres.add_argument('-H', '--host', default='localhost',
                          help='DNS name or IP address of the database instance to which to'
                               'connect')
    postgres.add_argument('-P', '--port',
                          help='Port on which the database instance is accepting connections.')
    postgres.add_argument('-u', '--user',
                          help='User name used to connect to the database instance')
    postgres.add_argument('-p', '--password',
                          help='Password used to connect to the database instance. Leaving this '
                               'blank may require the user to enter the password at a prompt.')
    postgres.add_argument('-S', '--schema', default='',
                          help='Schema to use within database')
    
    args = parser.parse_args(my_args)

    if args.dbpath is None and args.dbname is not None:
        dbtype = 'postgis'
    elif args.dbpath is not None and args.dbname is None:
        dbtype = 'spatialite'
    else:
        dbtype = None

    states, sc_df = get_db_soil_areas(dbtype=dbtype, schema=args.schema, dbpath=args.dbpath,
                                      user=args.user, dbname=args.dbname, port=args.port,
                                      host=args.host, password=args.password)

