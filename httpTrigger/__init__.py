import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO

def main(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Python HTTP trigger function processed a request.')

    # Set up the connection string and container name
    connect_str = "DefaultEndpointsProtocol=https;AccountName=onetimeonlydatastorage;AccountKey=SrYqpvpXpkgzgPWKbtrs3XyLMA2hsQ+gc554Zu1TlVONBbnGQmWk5N4vdFPZohHCw9cmmlhtbmuG+AStwJYEzA==;EndpointSuffix=core.windows.net"
    container_name = "dataforml"
    output_container_name = "modeltraindata"
    output_parquet_name = "combined_data.parquet"

    try:
        # Create a BlobServiceClient object
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Get the input container client
        input_container_client = blob_service_client.get_container_client(container_name)

        # Get the output container client
        output_container_client = blob_service_client.get_container_client(output_container_name)

        # Read the CSV files into dataframes
        dataframes = []
        read_csv_files = []  # List to store the names of successfully read CSV files
        file_count = 0  # Counter for the number of CSV files read

        for blob in input_container_client.list_blobs():
            if blob.name.startswith("stocks_") and blob.name.endswith(".csv"):
                try:
                    blob_data = input_container_client.get_blob_client(blob.name).download_blob()
                    content = blob_data.content_as_text()
                    # Create a StringIO object to read the content as a file-like object
                    string_io = StringIO(content)
                    df = pd.read_csv(string_io, delimiter=',')
                    dataframes.append(df)
                    read_csv_files.append(blob.name)
                    file_count += 1
                    logging.info(f"Number of document: {file_count}")
                    # if file_count == 5:  # Stop reading after 5 CSV files
                    #     break
                except Exception as e:
                    logging.error(f"Error reading {blob.name}: {str(e)}")

        # Check if any dataframes were successfully loaded
        if len(dataframes) > 0:
            # Concatenate the dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)

            # Print the head of the combined dataframe
            logging.info(combined_df.head())

            # Count the total number of rows
            total_rows = len(combined_df)
            logging.info(f"Total number of rows: {total_rows}")

            # Print the names of the read CSV files
            logging.info("Successfully read CSV files:")
            for csv_file in read_csv_files:
                logging.info(csv_file)

            # Convert the combined dataframe to Parquet format
            parquet_data = combined_df.to_parquet()

            # Upload the Parquet file to Azure Blob Storage
            blob_client = output_container_client.get_blob_client(output_parquet_name)
            blob_client.upload_blob(parquet_data, overwrite=True)

            logging.info("Train dataframe saved to Azure Blob Storage as combined_data.parquet")
            return func.HttpResponse("Data processing completed successfully.")
        else:
            logging.info("No valid dataframes found.")
            return func.HttpResponse("No valid dataframes found.")
    except Exception as e:
        logging.error(f"Error processing CSV files: {str(e)}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
