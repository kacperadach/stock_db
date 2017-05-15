from os import environ

from app.constants import DEV_ENV_VARS, PROD_ENV_VARS
from db import FinanceDB, ScheduleDB


def main_service(official=False):
    initialize_env_vars(official)
    while True:




def initialize_env_vars(official):
    environment_vars = PROD_ENV_VARS if official else DEV_ENV_VARS
    for key, val in environment_vars.items():
        environ[key] = val
