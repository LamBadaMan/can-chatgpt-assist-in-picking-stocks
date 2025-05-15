import pandas as pd
import numpy as np
import openai
from tqdm.auto import trange
import multiprocessing as mp
import backoff
import time
import os
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("API_KEY")


def get_key():
    return API_KEY


def return_prompt(statpers,fpedats,cname,ticker):
    statpers = pd.to_datetime(statpers).strftime("%B %d, %Y")
    fpedats = pd.to_datetime(fpedats).year
    prompt = f"Forget all previous instructions. Pretend to be an investment professional. Based on all information available as of {statpers}, what is your expected EPS of {cname} ({ticker}) for fiscal year {fpedats}? Just provide the estimate. Do not write a whole sentence. Do not provide other content."
    return prompt


@backoff.on_exception(backoff.expo,openai.error.RateLimitError,jitter=backoff.random_jitter,max_tries=3)
def gpt_query(statpers,fpedats,cname,ticker):
    prompt = return_prompt(statpers=statpers,fpedats=fpedats,cname=cname,ticker=ticker)
    openai.api_key = get_key()
    messages = [{"role": "user", "content": prompt}]
    completion = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=messages, temperature=0)
    content = completion.choices[0].message["content"]
    return content


def _get_eps(data,pb_pos=None):
    results = []
    if pb_pos is not None:
        iterator = trange(data.shape[0], position=pb_pos, leave=False)
        iterator.set_description(f"Job #{pb_pos}")
    else:
        iterator = range(data.shape[0])

    for row in iterator:
        time.sleep(2 + abs(np.random.normal(loc=0, scale=0.2, size=1)[0]))
        subset = data.iloc[[row]].copy()
        statpers = subset["statpers"].iloc[0]
        fpedats = subset["fpedats"].iloc[0]
        cname = subset["cname"].iloc[0]
        ticker = subset["ticker"].iloc[0]
        subset["eps_gpt"] = gpt_query(statpers,fpedats,cname,ticker)
        results.append(subset)

    results = pd.concat(results)
    return results


def get_eps(data,dir,pb=True):

    permno = data["permno"].iloc[0]
    fname = f"{dir}/{permno}.csv"

    if data.shape[0] < 7:
        njobs = data.shape[0]
    else:
        njobs = 7

    if njobs == 1:
        if pb:
            pb_pos = 0
        else:
            pb_pos = None

        results = _get_eps(data=data,pb_pos=pb_pos).sort_values(["permno", "statpers"])
        results.to_csv(fname, index=False)

    else:
        jobs = [x.tolist() for x in np.array_split(range(data.shape[0]), njobs)]

        if pb:
            arg_iterable = [(data.iloc[rows], pb_pos) for pb_pos, rows in enumerate(jobs)]
        else:
            arg_iterable = [(data.iloc[rows], pb_pos) for pb_pos, rows in jobs]

        with mp.Pool(njobs) as pool:
            results = pool.starmap(_get_eps, arg_iterable)

        results = pd.concat(results).sort_values(["permno", "statpers"])
        results.to_csv(fname, index=False)


def download_manager(temp_dir):

    ibes = pd.read_csv("XXXX.csv",parse_dates=["fpedats","statpers","anndats"]) \
        .sort_values(["permno","statpers"])

    permnos = ibes["permno"].drop_duplicates().to_list()
    for permno in permnos:
        fname = f"{temp_dir}/{permno}.csv"
        if os.path.exists(fname):
            continue
        else:
            print(f"PERMNO: {permno} --- Downloading...")
            time_start = time.time()
            data = ibes[ibes["permno"] == permno].sort_values(["permno","statpers"])
            get_eps(data=data,dir=temp_dir,pb=True)
            time_end = time.time()
            duration = round((time_end - time_start)/60,2)
            print(f"PERMNO: {permno} --- Done (Time Elapsed: {duration} min).")


