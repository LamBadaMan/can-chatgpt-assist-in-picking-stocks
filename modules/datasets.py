import vaex
import pandas as pd
import numpy as np
from tqdm.auto import tqdm
from pandas_datareader import famafrench
from modules import eps
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import os

pd.set_option("display.max_columns", None)


def generate_symbols():

    ffind49_path = "XXXX.csv"
    ffind49 = pd.read_csv(ffind49_path) \
        .filter(items=["sic", "ff_49ind"])


    path_crsp = "XXXX.hdf5"
    crsp = vaex.open(path_crsp)
    colnames = {x: x.lower() for x in crsp.get_column_names()}
    for col in colnames.items():
        crsp.rename(col[0], col[1])

    crsp = crsp[(crsp["date"] >= 19900000) & (crsp["date"] <= 20210000)]
    crsp = crsp[crsp["shrcd"].isin([10, 11])]
    crsp = crsp[(crsp["shrcls"].isna()) | (crsp["shrcls"] == "A")]
    crsp = crsp[(crsp["primexch"].isin(["N", "A", "Q"])) & (crsp["hexcd"].isin([1, 2, 3]))]

    cols2use = ["permno", "date", "permco", "ticker", "comnam", "prc", "shrout"]
    crsp = crsp[cols2use] \
        .to_pandas_df() \
        .drop_duplicates() \
        .assign(date=lambda x: pd.to_datetime(x["date"], format="%Y%m%d"),
                shrout=lambda x: x["shrout"] * 1000,
                prc=lambda x: x["prc"].abs(),
                mcap=lambda x: (x["prc"] * x["shrout"]) / 1_000_000) \
        .drop(columns=["shrout","prc"])

    # path_taqlinks = "/Volumes/T7A/databases/wrds_linking_tools/daily_taq_crsp.csv"
    # crsp2taq = pd.read_csv(path_taqlinks, parse_dates=["date"], low_memory=False) \
    #     .rename(columns=str.lower) \
    #     .query("match_lvl == 1") \
    #     .filter(items=["date", "permno", "sym_root", "sym_suffix", "cusip", "ncusip"])

    path_compustat = "XXXX.csv"
    cols2use = ["LPERMNO", "LPERMCO", "LINKDT", "LINKENDDT", "gvkey", "sic", "naics"]
    crsp2compustat = pd.read_csv(path_compustat, usecols=cols2use, dtype={"gvkey": "float32"}) \
        .rename(columns={"LPERMNO": "permno", "LPERMCO": "permco", "LINKDT": "ccm_startdt", "LINKENDDT": "ccm_enddt"}) \
        .rename(columns=str.lower)

    crsp2compustat["ccm_enddt"] = crsp2compustat["ccm_enddt"].replace("E", 20201231)
    crsp2compustat["ccm_enddt"] = pd.to_datetime(crsp2compustat["ccm_enddt"], format="%Y%m%d")
    crsp2compustat["ccm_startdt"] = pd.to_datetime(crsp2compustat["ccm_startdt"], format="%Y%m%d")


    symbols = crsp \
        .merge(crsp2compustat, how="inner", on=["permno", "permco"]) \
        .query("(date >= ccm_startdt) & (date <= ccm_enddt)") \
        .drop(columns=["ccm_startdt", "ccm_enddt"])


    nysebps_path = "XXXX.CSV"
    nysebps = pd.read_csv(nysebps_path, skiprows=1, header=None)
    nysebps.columns = ["date","no_idea"] + [f"bp_{str(x)}" for x in range(5, 105, 5)]
    nysebps = nysebps \
        .assign(date=lambda x: pd.to_datetime(x["date"], format="%Y%m"),
                year=lambda x: x["date"].dt.year,
                month=lambda x: x["date"].dt.month) \
        .filter(items=["year","month","bp_10","bp_90"]) \
        .sort_values(["year","month"])


    symbols = symbols \
        .assign(year=lambda x: x["date"].dt.year,
                month=lambda x: x["date"].dt.month) \
        .merge(nysebps, how="inner", on=["year", "month"])  \
        .assign(nanmcap_flag=lambda x: np.where(x['mcap'].isna(), 1, 0),
                microcap_flag=lambda x: np.where(x['mcap'] < x['bp_90'], 1, 0)) \
        .drop(columns=["year","month"])


    permnos2keep = symbols \
        .loc[(symbols["nanmcap_flag"] == 0) & (symbols["microcap_flag"] == 0), "permno"] \
        .drop_duplicates()


    symbols = symbols \
        .query("permno in @permnos2keep") \
        .set_index("date") \
        .groupby(["permno"]) \
        .resample("1M") \
        .agg({"permco":"last","ticker":"last","comnam":"last","gvkey":"last","mcap":"last","sic":"last"}) \
        .reset_index() \
        .sort_values(["permno","date"])


    symbols = symbols \
        .merge(ffind49, how="left", on=["sic"]) \
        .drop(columns=["sic"])


    path = 'XXXX.csv'
    cols2use = ["TICKER", "PERMNO", "sdate", "edate", "SCORE"]
    ibes_crsp_links = pd.read_csv(path, usecols=cols2use, parse_dates=["sdate", "edate"]) \
        .rename(columns=str.lower) \
        .query("score == 1") \
        .rename(columns={"ticker":"ticker_ibes"}) \
        .drop(columns=["score"])


    symbols = symbols \
        .merge(ibes_crsp_links, how="inner", on=["permno"]) \
        .query("(date >= sdate) & (date <= edate)") \
        .drop(columns=["sdate", "edate"])

    symbols.to_csv("data/symbols.csv", index=False)


def generate_ibes():

    cols2use = ["date","permno","permco","comnam","gvkey","ticker","ticker_ibes","ff_49ind"]
    symbols = pd.read_csv("data/symbols.csv", parse_dates=["date"], low_memory=False, usecols=cols2use) \
        .assign(year=lambda x: x["date"].dt.year,
                month=lambda x: x["date"].dt.month) \
        .drop(columns=["date"])


    ticker_ibes = symbols["ticker_ibes"].drop_duplicates()


    path = "XXXX.csv"
    actuals = pd.read_csv(path, parse_dates=["ANNDATS","ACTDATS","PENDS"])
    actuals.columns = [x.lower() for x in actuals.columns]
    actuals = actuals[actuals["measure"] == "EPS"]
    actuals = actuals[actuals["pdicity"] == "ANN"]
    actuals = actuals[actuals["curr_act"] == "USD"]
    actuals = actuals[actuals["ticker"].isin(ticker_ibes)]
    actuals = actuals.rename(columns={"pdicity":"fiscalp","pends":"fpedats","value":"actual"})
    actuals = actuals.drop(columns=["acttims","anntims","cname"])


    path = "XXXX.csv"
    sumstat = dd.read_csv(path, dtype="string")
    sumstat.columns = [x.lower() for x in sumstat.columns]
    sumstat = sumstat[sumstat["measure"] == "EPS"]
    sumstat = sumstat[sumstat["fiscalp"] == "ANN"]
    sumstat = sumstat[sumstat["curcode"] == "USD"]
    sumstat = sumstat[sumstat["fpi"] == "1"]
    sumstat = sumstat[sumstat["ticker"].isin(ticker_ibes)]

    with ProgressBar():
        sumstat = sumstat.compute()

    cols2date = ["statpers","fpedats"]
    for col in cols2date:
        sumstat[col] = pd.to_datetime(sumstat[col])

    cols2numeric = ["fpi", "numest", "numup", "numdown", "medest", "meanest", "stdev", "highest", "lowest", "usfirm"]
    for col in cols2numeric:
        sumstat[col] = pd.to_numeric(sumstat[col])

    cols2merge = ["ticker","cusip","oftic","measure","usfirm","fiscalp","fpedats"]
    cols2agg = ["statpers","anndats","cname","actual","numest","meanest","medest","stdev",
                "lowest","highest","curcode","curr_act"]

    ibes = sumstat \
        .merge(actuals, how="inner", on=cols2merge) \
        .sort_values(["ticker","fpedats","anndats","statpers"]) \
        .groupby(["ticker","fpedats"])[cols2agg] \
        .last() \
        .reset_index() \
        .assign(year=lambda x: x["statpers"].dt.year,
                month=lambda x: x["statpers"].dt.month) \
        .rename(columns={"ticker":"ticker_ibes"}) \
        .merge(symbols, how="inner", on=["year","month","ticker_ibes"]) \
        .sort_values(["permno","statpers"])

    permnos2keep = ibes["permno"].drop_duplicates()

    path_crsp = "XXXX.hdf5"
    crsp = vaex.open(path_crsp)
    colnames = {x: x.lower() for x in crsp.get_column_names()}
    for col in colnames.items():
        crsp.rename(col[0], col[1])

    crsp = crsp[(crsp["date"] >= 19900000) & (crsp["date"] <= 20210000)]
    crsp = crsp[crsp["permno"].isin(permnos2keep)]

    cols2use = ["permno", "date", "cfacshr"]
    crsp = crsp[cols2use] \
        .to_pandas_df() \
        .drop_duplicates() \
        .assign(date=lambda x: pd.to_datetime(x["date"], format="%Y%m%d"))

    crsp_est = crsp.rename(columns={"date":"statpers","cfacshr":"cfacshr_est"})
    crsp_rep = crsp.rename(columns={"date":"anndats","cfacshr":"cfacshr_rep"})

    ibes_corrected = ibes \
        .merge(crsp_est, how="inner", on=["permno","statpers"]) \
        .merge(crsp_rep, how="inner", on=["permno", "anndats"]) \
        .sort_values(["permno","statpers"])

    # ibes: 13_425
    # test: 13_254

    ibes_corrected["cfacshr_ratio"] = ibes_corrected["cfacshr_est"] / ibes_corrected["cfacshr_rep"]
    ibes_corrected["actual_corrected"] = ibes_corrected["actual"] * ibes_corrected["cfacshr_ratio"]

    colsorder = ["permno",
                 "permco",
                 "gvkey",
                 "ticker",
                 "ticker_ibes",
                 "comnam",
                 "cname",
                 "ff_49ind",
                 "fpedats",
                 "statpers",
                 "anndats",
                 "actual",
                 "actual_corrected",
                 "cfacshr_est",
                 "cfacshr_rep",
                 "cfacshr_ratio",
                 "numest",
                 "meanest",
                 "medest",
                 "stdev",
                 "lowest",
                 "highest",
                 "curcode",
                 "curr_act"
                 ]

    ibes_corrected = ibes_corrected[colsorder]
    # test = ibes_corrected[ibes_corrected["ticker"] == "SUNW"]
    ibes_corrected.to_csv("data/ibes.csv", index=False)


def generate_eps_est(temp_dir):

    def convert_eps(x):
        try:
            return pd.to_numeric(x)
        except:
            return np.nan

    files = [f"{temp_dir}/{x}" for x in os.listdir(f"{temp_dir}") if not x.startswith("._")]
    eps_est = dd.read_csv(files,parse_dates=["fpedats","statpers","anndats"]) \
        .compute() \
        .assign(eps_gpt=lambda x: x["eps_gpt"].str.replace("$", "").apply(lambda x: convert_eps(x)))

    eps_est.to_csv("data/eps_est.csv", index=False)

def generate_attractiveness(temp_dir):

    def convert_attractiveness(x):
        try:
            return pd.to_numeric(x)
        except:
            return np.nan

    files = [f"{temp_dir}/{x}" for x in os.listdir(f"{temp_dir}") if not x.startswith("._")]
    attractiveness = dd.read_csv(files,dtype={"attractiveness_gpt":str}) \
        .compute() \
        .assign(attractiveness_gpt=lambda x: x["attractiveness_gpt"].apply(lambda x: convert_attractiveness(x)))

    attractiveness.to_csv("data/attractiveness.csv", index=False)


def generate_market_data():
    symbols = pd.read_csv("data/symbols.csv", parse_dates=["date"], low_memory=False)
    permnos = symbols["permno"].drop_duplicates()

    path_crsp = "XXXX.hdf5"
    crsp = vaex.open(path_crsp)
    colnames = {x: x.lower() for x in crsp.get_column_names()}
    for col in colnames.items():
        crsp.rename(col[0], col[1])

    cols2use = ["permno", "date", "ret"]
    crsp = crsp[cols2use]

    market_data = []
    for permno in tqdm(permnos):
        sec_data = crsp[crsp["permno"] == permno] \
            .to_pandas_df() \
            .drop_duplicates() \
            .assign(date=lambda x: pd.to_datetime(x["date"], format="%Y%m%d"),
                    ret=lambda x: np.where(x['ret'].str.isalpha(), np.nan, x['ret']).astype('float')) \
            .query("date >= '1962-01-01'") \
            .query("date < '2021-01-01'")

        market_data.append(sec_data)

    market_data = pd.concat(market_data) \
        .sort_values(["permno","date"])

    market_data.to_csv("data/market_data.csv", index=False)

def generate_index_data():
    path_crsp = "XXXX.hdf5"
    crsp = vaex.open(path_crsp)
    colnames = {x: x.lower() for x in crsp.get_column_names()}
    for col in colnames.items():
        crsp.rename(col[0], col[1])

    crsp = crsp[(crsp["date"] >= 19620000) & (crsp["date"] <= 20210000)]
    crsp = crsp[crsp["shrcd"].isin([10, 11])]
    crsp = crsp[(crsp["shrcls"].isna()) | (crsp["shrcls"] == "A")]
    crsp = crsp[(crsp["primexch"].isin(["N"]))]

    cols2use = ["date", "vwretd", "ewretd", "sprtrn"]
    index_data = crsp[cols2use] \
        .to_pandas_df() \
        .drop_duplicates() \
        .assign(date=lambda x: pd.to_datetime(x["date"],format="%Y%m%d")) \
        .sort_values(["date"])

    index_data.to_csv("data/index_data.csv", index=False)

def generate_factors():
    ff5_data = famafrench.FamaFrenchReader("F-F_Research_Data_5_Factors_2x3_daily", start="1963-01-01", end=None).read()[0]
    mom_data = famafrench.FamaFrenchReader("F-F_Momentum_Factor_daily", start="1963-01-01", end=None).read()[0]
    factor_data = ff5_data \
        .merge(mom_data, how="inner", left_index=True, right_index=True) \
        .rename(columns=lambda x: x.lower().replace("-","").strip()) \
        .rename_axis(index=lambda x: x.lower()) \
        .apply(lambda x: x / 100) \
        .reset_index()

    factor_data.to_csv("data/factors.csv", index=False)


