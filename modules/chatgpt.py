import datetime
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
from datetime import datetime, timedelta
import logging
import re
import backoff

# Configure a custom logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a custom handler to print to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

# logging.basicConfig(filename="file.log",
#                     level=logging.ERROR,
#                     format="%(asctime)s|%(levelname)s|%(name)s|%(message)s")


class CHATGPT:
        def __init__(self,headless):
                self.start_driver(headless)


        def start_driver(self, headless):
                prefs = {"credentials_enable_service": False,
                         "profile.password_manager_enabled": False}

                options = uc.ChromeOptions()
                options.add_experimental_option("prefs", prefs)
                options.headless = headless
                self.driver = uc.Chrome(use_subprocess=True, options=options)
                self.driver.maximize_window()
                self.actions = ActionChains(self.driver)
                self.driver.implicitly_wait(5)


        def get_start_page(self):
                self.driver.get("https://chat.openai.com/auth/login")


        def quit_driver(self):
                self.driver.quit()


        def __check_existance(self, by, selector):
                try:
                        self.driver.find_element(by, selector)
                except NoSuchElementException:
                        return False
                return True


        def login(self, account_name, password):

                login_button = '//*[@id="__next"]/div[1]/div[1]/div[4]/button[1]/div'
                WebDriverWait(self.driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH,login_button)))
                self.driver.find_element(By.XPATH,login_button).click()

                if "googlemail" in account_name:
                        google_login = "/html/body/div/main/section/div/div/div/div[4]/form[2]"
                        WebDriverWait(self.driver, timeout=10) \
                                .until(EC.element_to_be_clickable((By.XPATH, google_login))) \
                                .click()

                        input_user = "identifier"
                        input_user = WebDriverWait(self.driver, timeout=10) \
                                .until(EC.element_to_be_clickable((By.NAME, input_user)))

                        input_user.send_keys(account_name)
                        input_user.send_keys(Keys.ENTER)

                        input_pw = "Passwd"
                        input_pw = WebDriverWait(self.driver, timeout=10) \
                                .until(EC.element_to_be_clickable((By.NAME, input_pw)))

                        input_pw.send_keys(password)
                        input_pw.send_keys(Keys.ENTER)

                else:
                        input_user = "username"
                        input_user = WebDriverWait(self.driver, timeout=10) \
                                .until(EC.element_to_be_clickable((By.NAME, input_user)))
                        input_user.send_keys(account_name)
                        input_user.send_keys(Keys.ENTER)

                        input_pw = "password"
                        input_pw = WebDriverWait(self.driver, timeout=10) \
                                .until(EC.element_to_be_clickable((By.NAME, input_pw)))
                        input_pw.send_keys(password)
                        input_pw.send_keys(Keys.ENTER)

                time.sleep(3)
                first_button = '//*[@id="radix-:r9:"]/div[2]/div/div[2]/button/div'
                WebDriverWait(self.driver, timeout=10) \
                        .until(EC.element_to_be_clickable((By.XPATH, first_button))).click()

                second_button = '//*[@id="radix-:r9:"]/div[2]/div/div[2]/button[2]/div'
                WebDriverWait(self.driver, timeout=10) \
                        .until(EC.element_to_be_clickable((By.XPATH, second_button))).click()

                third_button = '//*[@id="radix-:r9:"]/div[2]/div/div[2]/button[2]/div'
                WebDriverWait(self.driver, timeout=10) \
                        .until(EC.element_to_be_clickable((By.XPATH, third_button))).click()

                if self.__check_existance(By.PARTIAL_LINK_TEXT, "Upgrade to Plus"):
                        print("No paid account. Rotating user...")
                        return False
                else:
                        return True


        def __click_menu(self):
                menu_button = "//*[contains(@id,'headlessui-menu-button')]"
                self.driver.find_element(By.XPATH, menu_button).click()


        def logout(self):
                self.__click_menu()
                WebDriverWait(self.driver, timeout=10) \
                        .until(EC.visibility_of_element_located((By.LINK_TEXT, "Log out"))) \
                        .click()


        def check_settings(self):
                self.__click_menu()

                settings_button = "Settings"
                WebDriverWait(self.driver, timeout=10) \
                        .until(EC.visibility_of_element_located((By.LINK_TEXT, settings_button))) \
                        .click()

                beta_features = "button[id*='trigger-BetaFeatures']"
                WebDriverWait(self.driver, timeout=10) \
                        .until(EC.visibility_of_element_located((By.CSS_SELECTOR, beta_features))) \
                        .click()

                toggles = self.driver.find_elements(By.TAG_NAME, "button")

                beta_toggle = [x for x in toggles if x.get_attribute("aria-label") == "Browse with Bing"][0]
                state = beta_toggle.get_attribute("data-state")
                if state == "unchecked":
                        beta_toggle.click()

                close_dialog = "inline-block text-gray-500 hover:text-gray-700"
                close_dialog = [x for x in toggles if x.get_attribute("class") == close_dialog][0]
                close_dialog.click()


        def __get_history(self):
                model = self.driver.find_element(By.XPATH, "//div[contains(text(), 'Model:')]").text
                divs = self.driver.find_elements(By.XPATH,"//div[contains(@class, 'items-start')]")
                message = divs[1].text
                completion = divs[2].text
                return (model, message, completion)


        @backoff.on_exception(backoff.constant, Exception, interval=3, max_tries=5)
        def start_chat(self, prompt, cushion=5, max_wait=60):
                self.driver.get("https://chat.openai.com/")
                time.sleep(cushion)
                model_selection = self.driver.find_elements(By.TAG_NAME, "button")
                model_selection = [x for x in model_selection if "radix" in x.get_attribute("id")][1]
                self.actions.move_to_element(model_selection).perform()
                model_selection.click()
                self.actions.move_to_element(model_selection).perform()
                WebDriverWait(self.driver, timeout=10) \
                        .until(EC.visibility_of_element_located((By.XPATH,"//span[contains(text(), 'Browse with')]"))) \
                        .click()
                # self.driver.find_elements(By.XPATH, "//span[contains(text(), 'Browse with')]")[0].click()
                time.sleep(cushion)
                text_area = WebDriverWait(self.driver, timeout=10) \
                        .until(EC.element_to_be_clickable((By.TAG_NAME, "textarea")))

                text_area.send_keys(prompt)
                text_area.send_keys(Keys.ENTER)
                check_response = self.driver.find_elements(By.XPATH, "//div[contains(text(), 'Use default model')]")
                if check_response:
                        time_string = self.driver. \
                                find_element(By.XPATH, "//span[contains(text(), 'usage cap')]").text

                        time_string = re.findall(r"(\d{1,2}:\d{2} [APM]{2})", time_string)[0]

                        current_datetime = datetime.now()
                        datetime_object = datetime.strptime(time_string, "%I:%M %p")
                        if current_datetime.strftime("%p") == "PM" and datetime_object.strftime("%p") == "AM":
                                datetime_object += timedelta(days=1)
                                datetime_object = datetime_object.replace(
                                        hour=datetime_object.hour % 12)  # Adjust the hour to be in 12-hour format

                        datetime_object = datetime_object.replace(year=current_datetime.year,
                                                                  month=current_datetime.month,
                                                                  day=current_datetime.day)

                        return {"success_flag":False, "content":datetime_object}
                else:
                        success = False
                        start_time = time.time()
                        time_passed = 0
                        while (success is False) and (time_passed <= max_wait):
                                time_passed = time.time() - start_time
                                time.sleep(2)
                                try:
                                        pb = [x for x in self.driver.find_elements(By.TAG_NAME, "button") if
                                              x.get_attribute("as") == "button"][0]
                                        if pb.text == "Regenerate response":
                                                success = True
                                except:
                                        pass

                        return {"success_flag":True, "content":self.__get_history()}



class OpenaiLimitReached(Exception):
    pass

class OpenaiLoginError(Exception):
    pass

