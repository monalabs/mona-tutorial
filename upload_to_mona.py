import gzip
import json
import os

from mona_sdk.client import Client, MonaSingleMessage

# Get your Mona api_key and secret.
api_key = os.environ.get("MONA_API_KEY", "")
secret = os.environ.get("MONA_SECRET", "")

# Initiate Mona's client.
my_mona_client = Client(api_key, secret)


def read_dicts_from_zip(file_name):
    # Read from zipped file.
    with gzip.open(file_name, "r") as f:
        json_bytes = f.read()

    json_str = json_bytes.decode("utf-8")
    return json.loads(json_str)


def send_basic_loans_data_to_mona():
    # We'll send the basic data, one file (containing a list of dicts) per day, saved in
    # "loans_basic_data" directory.

    export_result = []

    for single_day_data_file_name in [
        file for file in os.listdir("loans_basic_data") if file.endswith(".gzip")
    ]:

        # Get the data from the current file.
        loans_cases_data = read_dicts_from_zip(
            f"loans_basic_data/{single_day_data_file_name}"
        )

        # loans_cases_data is a list of dicts, where each dict represent a single case of a
        # loan.
        loans_cases_data_to_send = []
        for single_loan_data in loans_cases_data:
            loans_cases_data_to_send.append(
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
                            "risk_score",
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
                    contextClass="RISK_MONITORING",
                    # contextID should be a unique identifier of each loan case, later when
                    # we'll add additional information for a specific loan, we'll use the
                    # same id so Mona's servers can aggregate them properly.
                    contextId=single_loan_data["id"],
                    exportTimestamp=single_loan_data["timestamp"],
                )
            )

        # Export batch to Mona's servers.
        export_result.append(my_mona_client.export_batch(loans_cases_data_to_send))
    return all(export_result)


# Now, lets send additional information: was the loan returned on time or not.
# This information is saved in a similar way to the basic data, in the
# "loans_paid_back_stat" directory.
for single_day_data_file_name in [
    file for file in os.listdir("loans_paid_back_stat") if file.endswith(".gzip")
]:

    # Get the data from the current file.
    loans_return_status_data = read_dicts_from_zip(
        f"loans_paid_back_stat/{single_day_data_file_name}"
    )

    loans_return_status_data_to_send = []
    for single_loan_data in loans_return_status_data:
        loans_return_status_data_to_send.append(
            MonaSingleMessage(
                # Now, the message field will contain the information we want to add for
                # each loan.
                message={"loan_paid_back": single_loan_data["loan_paid_back"]},
                contextClass="RISK_MONITORING",
                # As mentioned before, the id here matches a loan from the previous
                # batch, so this new information will be connected to this specific
                # loan.
                contextId=single_loan_data["id"],
                exportTimestamp=single_loan_data["timestamp"],
            )
        )

    # Export batch to Mona's servers.
    export_result = my_mona_client.export_batch(loans_return_status_data_to_send)


# We also have training data for both of our models, lets send the train and test sets
# for each of them:
for training_file_name in [
    file for file in os.listdir("training") if file.endswith(".gzip")
]:
    training_data = read_dicts_from_zip(f"training/{training_file_name}")

    training_data_to_send = []
    for single_loan_data in training_data:
        training_data_to_send.append(
            MonaSingleMessage(
                message={
                    key: single_loan_data[key]
                    for key in [
                        "occupation",
                        "city",
                        "state",
                        "purpose",
                        "risk_score",
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
                        "loan_paid_back",
                    ]
                    if key in single_loan_data
                },
                contextClass="RISK_MONITORING",
                # contextID should be a unique identifier of each loan case, later when
                # we'll add additional information for a specific loan, we'll use the
                # same id so Mona's servers can aggregate them properly.
                contextId=single_loan_data["id"],
                exportTimestamp=single_loan_data["timestamp"],
            )
        )

    # Export batch to Mona's servers.
    export_result = my_mona_client.export_batch(training_data_to_send)
