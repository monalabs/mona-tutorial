import logging
import os

from mona_sdk.client import Client, MonaSingleMessage

from util import read_dicts_from_zip

# Get your Mona api_key and secret.
api_key = os.environ.get("MONA_API_KEY", "")
secret = os.environ.get("MONA_SECRET", "")

# Initiate Mona's client.
my_mona_client = Client(api_key, secret)

logger = logging.getLogger()
logger.setLevel("INFO")

context_class_name = os.environ.get("CONTEXT_CLASS_NAME", "LOAN_APPLICATION_TUTORIAL")

# Send the training data (train and test) for both of our model versions.
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
                contextClass=context_class_name,
                contextId=single_loan_data["id"],
                exportTimestamp=single_loan_data["timestamp"],
            )
        )

    # Export the batch to Mona's servers.
    logging.info(f"Exporting a batch of {len(training_data_to_send)}")
    export_result = my_mona_client.export_batch(training_data_to_send)
    logging.info(f"Export result is: {export_result}")
