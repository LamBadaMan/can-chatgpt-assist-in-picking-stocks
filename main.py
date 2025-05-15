import time
import pandas as pd
import numpy as np
from modules.chatgpt import CHATGPT, OpenaiLoginError, OpenaiLimitReached
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from modules.db import initialize_engine, add_history, update_activity, get_random_account
import backoff
from datetime import datetime
from modules.datasets import generate_market_data, generate_index_data, generate_attractiveness, generate_eps_est
from modules.attractiveness import download_manager as attractiveness_dm
from modules.eps import download_manager as eps_dm
from modules.db import initialize_db

pd.set_option("display.max_columns", None)

ERRORS = (NoSuchElementException, TimeoutException, OpenaiLimitReached, OpenaiLoginError)

def countdown(t, message):
    while t:
        mins, secs = divmod(t, 60)
        timer = '{}: {:02d}:{:02d}'.format(message, mins, secs)
        print(timer, end="\r")
        time.sleep(1)
        t -= 1


@backoff.on_exception(backoff.constant,ERRORS,interval=3,max_tries=5)
def chatgpt_queries(prompt):
    engine = initialize_engine("accounts.db")

    success = False
    while (success is False):
        response = get_random_account(engine)
        if response["success_flag"]:
            success = True
        else:
            countdown(120,"Retrying in")

    account, activity = response["query"]
    if account.id:
        test = CHATGPT(headless=False)
        test.get_start_page()
        if test.login(account.account_name, account.password):
            update_activity(engine, account.account_name, datetime.now())
            test.check_settings()
            for x in range(25):
                print(x)
                result = test.start_chat(prompt)
                if result["success_flag"]:
                    model, message, completion = result["content"]
                    add_history(engine, account.id, model, message, completion)
                    time.sleep(2)
                else:
                    print("Rotating user.")
                    reset_time = result["content"]
                    update_activity(engine,
                                    account_name=account.account_name,
                                    api_limit_hit=datetime.now(),
                                    api_limit_reset=reset_time)
                    raise OpenaiLimitReached
        else:
            test.logout()
            test.driver.quit()
            raise OpenaiLoginError

        test.logout()
        test.driver.quit()




if __name__ == '__main__':

    initialize_db(replace=True)

    generate_market_data()
    generate_index_data()

    temp_dir = "data/temp/attractiveness"
    attractiveness_dm(temp_dir)
    generate_attractiveness(temp_dir)

    temp_dir = "data/temp/eps"
    eps_dm(temp_dir)
    generate_eps_est(temp_dir)