# Databricks notebook source
import time
from functools import wraps


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
        print(f"Function {f.__name__} took: {round(te-ts,2)} seconds")
        return result

    return wrap


# COMMAND ----------
