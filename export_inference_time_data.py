import logging
import os

from mona_sdk.client import Client, MonaSingleMessage

from util import read_dicts_from_zip

# Get your Mona api_key and secret.
api_key = os.environ.get("MONA_API_KEY", "")
secret = os.environ.get("MONA_SECRET", "")

# Initiate Mona's client.
my_mona_client = Client(api_key, secret)

# Set your own logger.
logger = logging.getLogger()
logger.setLevel("INFO")

# You can also get mona-logger as follows and set its level.
mona_logger = logging.getLogger("mona-logger")
mona_logger.setLevel("WARNING")

# You can set the context class name using "CONTEXT_CLASS_NAME" env var.
context_class_name = os.environ.get("CONTEXT_CLASS_NAME", "LOAN_APPLICATION_TUTORIAL")

# We'll send the inference time data, one file (containing a list of dicts) per day,
# saved in "loans_inference_time_data" directory.
for single_day_data_file_name in [
    file for file in os.listdir("loans_inference_time_data") if file.endswith(".gzip")
]:
    # Get the data from the current file (holds a full day's data).
    loans_cases_data = read_dicts_from_zip(
        f"loans_inference_time_data/{single_day_data_file_name}"
    )
    # Now, loans_cases_data is a list of dicts, where each dict represent a single case
    # of a loan.

    # loans_cases_data_to_send will hold all MonaSingleMessages with the features we
    # want to export to mona.
    loans_cases_data_to_send = []
    for single_loan_data in loans_cases_data:
        loans_cases_data_to_send.append(
            # Create a MonaSingleMessage for each loan dict.
            MonaSingleMessage(
                # The message field should be a dict containing all feature we want to
                # monitor.
                message={
                    key: single_loan_data[key]
                    for key in [
                        "occupation",
                        "city",
                        "state",
                        "purpose",
                        "credit_score",
                        "loan_taken",
                        "return_until",
                        "offered_amount",
                        "approved_amount",
                        "feature_0",
                        "feature_1",
                        "feature_2",
                        "feature_3",
                        "feature_4",
                        "feature_5",
                        "feature_6",
                        "feature_7",
                        "feature_8",
                        "feature_9",
                        "stage",
                        "model_version",
                    ]
                    if key in single_loan_data
                },
                contextClass=context_class_name,
                # contextID should be a unique identifier of each loan case, later when
                # we'll add additional information for a specific loan, we'll use the
                # same id so Mona's servers can join them to a single loan.
                contextId=single_loan_data["id"],
                # exportTimestamp is the primary timestamp Mona will use when
                # considering the data being sent. It should be a date (ISO string, or a
                # Unix time number) representing the time the message was created.
                exportTimestamp=single_loan_data["timestamp"],
            )
        )

    # Export the batch to Mona's servers.
    logging.info(f"Exporting a batch of {len(loans_cases_data_to_send)}")
    export_result = my_mona_client.export_batch(loans_cases_data_to_send)
    logging.info(f"Export result is: {export_result}")
