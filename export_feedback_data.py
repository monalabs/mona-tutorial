import logging
import os

from mona_sdk import Client, MonaSingleMessage

from util import read_dicts_from_zip

# Get your Mona api_key and secret.
api_key = os.environ.get("MONA_API_KEY", "")
secret = os.environ.get("MONA_SECRET", "")

# Initiate Mona's client.
my_mona_client = Client(api_key, secret)

my_logger = logging.getLogger()
my_logger.setLevel("INFO")

context_class_name = os.environ.get("CONTEXT_CLASS_NAME", "LOAN_APPLICATION_TUTORIAL")

# Send additional information: was the loan returned on time or not.
for single_day_data_file_name in [
    file for file in os.listdir("loans_feedback_status") if file.endswith(".gzip")
]:
    loans_return_status_data = read_dicts_from_zip(
        f"loans_feedback_status/{single_day_data_file_name}"
    )

    loans_return_status_data_to_send = []

    for single_loan_data in loans_return_status_data:
        loans_return_status_data_to_send.append(
            MonaSingleMessage(
                # Now, the message field will contain the information we want to add
                # for each loan.
                message={"loan_paid_back": single_loan_data["loan_paid_back"]},
                contextClass=context_class_name,
                # As mentioned before, the id here matches a loan from the previous
                # batch, so this new information will be connected to this specific
                # loan.
                contextId=single_loan_data["id"],
                exportTimestamp=single_loan_data["timestamp"],
            )
        )

    # Export the batch to Mona's servers.
    logging.info(f"Exporting a batch of {len(loans_return_status_data_to_send)}")
    export_result = my_mona_client.export_batch(loans_return_status_data_to_send)
    logging.info(f"Export result is: {export_result}")
