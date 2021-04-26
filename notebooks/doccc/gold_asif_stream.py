# Databricks notebook source
"""Usage: 
# COMMAND ----------
%run '/notebooks/doccc/gold_asif_stream.py'
# COMMAND ----------
# Either
GMSAL = GoldMockStreamAutoLinear(<kwargs>)
GMSAL.start_auto()
# or 
GMS = GoldMockStream(<kwargs>)
# And run manually for each step
GMS.gold_append())
# To clean up:
dbutils.fs.rm(<gold-path>, True)
"""

# COMMAND ----------

import time
from datetime import datetime, timedelta

from pyspark.sql.functions import (
    DataFrame,
    col,
    isnull,
    monotonically_increasing_id,
    rand,
    when,
)
from pytz import timezone

# COMMAND ----------


class GoldMockStream:
    """
    Class for generating mock gold table using asif updates from silver

    Parameters
    ----------
    full_data : DataFrame
        Data frame source to generate mock gold table. Typically a silver table.
    first_bulk_size : int
        How many data points should be used for initial mock table. Must
        be less than or equal to size of source table.
    validation_size : int
        How many rows should be kept for validation and never appended to gold
        training set
    gold_path : str
        Where to write gold mock data
    validation_path : str
        Where to write validation data
    num_steps : int, optional
        How many steps should be taken from the initial gold table
        to the full dataset excluding validation data.
        By default -1, meaning it is calculated using batch size.
        Cannot be set when batch_size is set.
        Must be set when batch_size is not set.
    batch_size : int, optional
        How many new rows should be appended to mock gold
        for each step.
        By default -1, meaning it is calculated using num_steps.
        Cannot be set when num_steps is set.
        Must be set when num_steps is not set.
    """

    def __init__(
        self,
        full_data: DataFrame,
        first_bulk_size: int,
        validation_size: int,
        gold_path: str,
        validation_path: str,
        num_steps: int = -1,
        batch_size: int = -1,
    ):
        self.full_data = full_data.cache()
        self.first_bulk_size = first_bulk_size
        self.validation_size = validation_size
        self.n = full_data.count()
        self.step = 0
        self.gold_path = gold_path
        self.validation_path = validation_path

        if num_steps == -1 and batch_size == -1:
            raise ValueError("Either num_steps or batch_size must be set")
        elif num_steps != -1 and batch_size != -1:
            raise ValueError("Cannot set both num_steps and batch size")
        elif num_steps != -1:
            self.num_steps = num_steps
            self.batch_size = int(
                (self.n - self.validation_size - self.first_bulk_size) / num_steps
            )
        elif batch_size != -1:
            self.batch_size = batch_size
            self.num_steps = int(
                (self.n - self.validation_size - self.first_bulk_size) / batch_size
            )

        self.first_bulk, self.validation, self.remaining = self.order_random()
        self.init_gold(verbose=True)

    def order_random(self):
        """Randomly splits data into validation, first bulk, and remaining data with groups

        Returns
        -------
        DataFrames as tuple
        """
        data_with_random_order = (
            self.full_data.orderBy(rand())
            .withColumn("sampled_order", monotonically_increasing_id())
            .withColumn(
                "group",
                when(col("sampled_order") < self.first_bulk_size, "first bulk").when(
                    col("sampled_order") >= (self.n - self.validation_size),
                    "validation",
                ),
            )
        )

        first_bulk = (
            data_with_random_order.filter(col("group") == "first bulk")
            .drop("group")
            .drop("sampled_order")
        )
        validation = (
            data_with_random_order.filter(col("group") == "validation")
            .drop("group")
            .drop("sampled_order")
        )
        remaining = data_with_random_order.filter(isnull(col("group"))).drop("group")

        return first_bulk, validation, remaining

    def init_gold(self, verbose: bool = False):
        """

        Initialize gold table using batch of size first_bulk_size


        Parameters
        ----------
        verbose : bool, optional
            Verbose, by default False
        """
        if verbose:
            print(
                f"Saving first batch of data. Total number of rows. {self.first_bulk_size}"
            )
        self.first_bulk.write.format("delta").mode("overwrite").save(self.gold_path)

    def save_validation(self, verbose: bool = False):
        """Generate validation table

        Parameters
        ----------
        verbose : bool, optional
            Verbose write, by default False
        """
        if verbose:
            print(
                f"Saving validation data. Total number of rows. {self.validation_size}"
            )
        self.validation.write.format("delta").mode("overwrite").save(
            self.validation_path
        )

    def gold_append(self, verbose: bool = False):
        """Appends new batch of data to mock gold table

        Parameters
        ----------
        verbose : bool, optional
            Verbose appends, by default False
        """
        step = self.step
        if step < self.num_steps:

            from_index = self.first_bulk_size + (self.step) * (self.batch_size)
            to_index = from_index + self.batch_size

            fold = self.remaining.filter(
                col("sampled_order").between(from_index, to_index - 1)
            )
            fold.drop("sampled_order").write.format("delta").mode("append").save(
                self.gold_path
            )

            if verbose:
                print(
                    f"Appending random data fold {self.step+1}/{self.num_steps}. Total number of rows {self.first_bulk_size + (self.step+1)*self.batch_size}"
                )
            self.step += 1
        else:
            if verbose:
                print("No more data to append")
            self.step += 1


# COMMAND ----------


class GoldMockStreamAutoLinear(GoldMockStream):
    """
    Class for generating mock gold table using appends over a set
    period of time


    Parameters
    ----------
    duration_minutes : int
        How long should the full procedure aim to use from the first batch
        to the exhaustive write to mock gold, excluding validation data
    full_data : DataFrame
        Data frame source to generate mock gold table. Typically a silver table.
    first_bulk_size : int
        How many data points should be used for initial mock table. Must
        be less than or equal to size of source table.
    validation_size : int
        How many rows should be kept for validation and never appended to gold
        training set
    gold_path : str
        Where to write gold mock data
    validation_path : str
        Where to write validation data
    batch_size : int, optional
        How many new rows should be appended to mock gold
        for each step. Larger number will provide speedup,
        as random slicing is performed somewhat inefficiently

    See also
    ----------
    GoldMockStream

    """

    def __init__(
        self,
        duration_minutes: int,
        full_data: DataFrame,
        first_bulk_size: int,
        validation_size: int,
        batch_size: int,
        verbose: bool = True,
    ):
        super().__init__(
            full_data=full_data,
            first_bulk_size=first_bulk_size,
            validation_size=validation_size,
            batch_size=batch_size,
        )
        self.duration_minutes = duration_minutes
        self.duration_seconds = 60 * duration_minutes
        self.verbose = verbose
        self.wait_time = 0.9 * self.duration_seconds / self.num_steps

    def print_config(self):
        """
        Print configuration details for GoldMockStreamAutoLinear
        """
        print(f"Duration seconds: {self.duration_seconds}")
        print(f"Number of steps: {self.num_steps}")
        print(f"Batch size: {self.batch_size}")
        print(f"Min wait time: {self.wait_time}")

    def start_auto(self):
        norway = timezone("Europe/Oslo")
        norway_time_start = datetime.now(norway)
        time_string = norway_time_start.strftime("%H:%M:%S")

        while self.step < self.num_steps:
            time.sleep(self.wait_time)

            aim_time = norway_time_start + timedelta(
                seconds=self.duration_seconds * self.step / self.num_steps
            )
            norway_time = datetime.now(norway)

            if norway_time > aim_time:
                self.wait_time *= 0.9
            else:
                self.wait_time *= 1.1

            self.gold_append()
            time_string = norway_time.strftime("%H:%M:%S")
            if self.verbose:
                print(f"Added {self.batch_size} new observations at {time_string}")
        norway_time = datetime.now(norway)
        time_string = norway_time.strftime("%H:%M:%S")
        if self.verbose:
            print(f"Finished writing at {time_string}")
