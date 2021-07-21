import logging
import os

from mona_sdk import Client, MonaSingleMessage

from util import read_dicts_from_zip

api_key = os.environ.get("MONA_API_KEY", "")
secret = os.environ.get("MONA_SECRET", "")

# Initiate Mona's client.
my_mona_client = Client(api_key, secret)

my_logger = logging.getLogger()
my_logger.setLevel("INFO")


# Send additional information: was the loan returned on time or not.
for single_day_data_file_name in [
    file for file in os.listdir("loans_paid_back_stat") if file.endswith(".gzip")
]:
    # Get the data from the current file.
    loans_return_status_data = read_dicts_from_zip(
        f"loans_paid_back_stat/{single_day_data_file_name}"
    )

    # loans_return_status_data_to_send will hold all MonaSingleMessages with the
    # features we want to export to mona.
    loans_return_status_data_to_send = []

    for single_loan_data in loans_return_status_data:
        loans_return_status_data_to_send.append(
            MonaSingleMessage(
                # Now, the message field will contain the information we want to add
                # for each loan.
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
    logging.info(f"Exporting a batch of {len(loans_return_status_data_to_send)}")
    export_result = my_mona_client.export_batch(loans_return_status_data_to_send)
    logging.info(f"Export result is: {export_result}")
