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

ERRORS = (openai.error.RateLimitError,openai.error.APIError,openai.error.APIConnectionError)

def get_key():
    return API_KEY


def return_prompt(formation_date,comnam,ticker):
    date = pd.to_datetime(formation_date).strftime("%B %d, %Y")
    prompt = f"Forget all previous instructions. Pretend to be an investment professional. Pretend that it is {date} and disregard any information beyond this date. Based on the available information as of {date}, how do you assess the relative attractiveness of {comnam} ({ticker}) for investors intending to hold this stock in the following month? Compare the stock to all other S&P 500 stocks and assign a score on a scale from 1 to 10, where 1 indicates very unattractive and 10 indicates very attractive. Only provide the score, without using complete sentences or additional content."
    return prompt


@backoff.on_exception(backoff.constant,ERRORS,interval=3,max_tries=5)
def gpt_query(formation_date,comnam,ticker):
    prompt = return_prompt(formation_date=formation_date,comnam=comnam,ticker=ticker)
    openai.api_key = get_key()
    messages = [{"role": "user", "content": prompt}]
    completion = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=messages, temperature=0)
    content = completion.choices[0].message["content"]
    return content


def _get_attractiveness(data,pb_pos=None):
    results = []
    if pb_pos is not None:
        iterator = trange(data.shape[0], position=pb_pos, leave=False)
        iterator.set_description(f"Job #{pb_pos}")
    else:
        iterator = range(data.shape[0])

    for row in iterator:
        time.sleep(2 + abs(np.random.normal(loc=0, scale=0.2, size=1)[0]))
        subset = data.iloc[[row]].copy()
        formation_date = subset["date"].iloc[0]
        comnam = subset["comnam"].iloc[0]
        ticker = subset["ticker"].iloc[0]
        subset["attractiveness_gpt"] = gpt_query(formation_date, comnam, ticker)
        results.append(subset)

    results = pd.concat(results)
    return results


def get_attractiveness(data,dir,pb=True):

    permno = data["permno"].iloc[0]
    fname = f"{dir}/{permno}.csv"

    if data.shape[0] <= 7:
        njobs = data.shape[0]
    else:
        njobs = 4

    if njobs == 1:
        if pb:
            pb_pos = 0
        else:
            pb_pos = None

        results = _get_attractiveness(data=data,pb_pos=pb_pos).sort_values(["permno", "date"])
        results.to_csv(fname, index=False)

    else:
        jobs = [x.tolist() for x in np.array_split(range(data.shape[0]), njobs)]

        if pb:
            arg_iterable = [(data.iloc[rows], pb_pos) for pb_pos, rows in enumerate(jobs)]
        else:
            arg_iterable = [(data.iloc[rows], pb_pos) for pb_pos, rows in jobs]

        with mp.Pool(njobs) as pool:
            results = pool.starmap(_get_attractiveness, arg_iterable)

        results = pd.concat(results).sort_values(["permno", "date"])
        results.to_csv(fname, index=False)


def download_manager(temp_dir):
    symbols = pd.read_csv("data/symbols.csv",parse_dates=["date"]).sort_values(["permno","date"])
    permnos = symbols["permno"].drop_duplicates().to_list()
    for permno in permnos:
        fname = f"{temp_dir}/{permno}.csv"
        if os.path.exists(fname):
            continue
        else:
            print(f"PERMNO: {permno} --- Downloading...")
            time_start = time.time()
            data = symbols[symbols["permno"] == permno].sort_values(["permno","date"])
            get_attractiveness(data=data,dir=temp_dir,pb=True)
            time_end = time.time()
            duration = round((time_end - time_start)/60,2)
            print(f"PERMNO: {permno} --- Done (Time Elapsed: {duration} min).")
            # os.system('clear')


