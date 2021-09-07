import argparse
import datetime
import gzip
import json
import os
import pathlib
import traceback

import powerschool
from dotenv import load_dotenv
from google.cloud import storage
from powerschool import utils

PROJECT_PATH = pathlib.Path(__file__).absolute().parent


def main(host, env_file, query_file):
    host_clean = host.replace(".", "_")

    env_path = PROJECT_PATH / ".envs" / host_clean / env_file
    queries_path = PROJECT_PATH / "queries" / host_clean / query_file

    load_dotenv(dotenv_path=env_path)

    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    token_file = os.getenv("TOKEN_FILE")
    current_yearid = os.getenv("CURRENT_YEARID")
    gcs_bucket_name = os.getenv("GCS_BUCKET_NAME")

    token_file = PROJECT_PATH / ".tokens" / host_clean / token_file
    client_credentials = (client_id, client_secret)
    current_yearid = int(current_yearid)

    # check if access token file exists
    if token_file.exists():
        print("Loading saved access token...")
        token_dict = json.load(token_file.open("r"))

        now = datetime.datetime.utcnow()
        expires_at = datetime.datetime.fromtimestamp(token_dict["expires_at"])
        token_remaining = expires_at - now
    else:
        print(f"Specified token file {token_file} does not exist!")
        if not token_file.parent.exists():
            print(f"\tCreating folder {'/'.join(token_file.parent.parts[-2:])}...")
            token_file.parent.mkdir(parents=True)
        token_dict = {}

    # check token validity and authenticate
    try:
        if token_remaining.days < 1:
            raise
            print("Token expired!")
        else:
            ps = powerschool.PowerSchool(host=host, auth=token_dict)
    except Exception:
        ps = powerschool.PowerSchool(host=host, auth=client_credentials)

    # save access token
    print(f"Saving access token to {'/'.join(token_file.parts[-3:])}...")
    json.dump(ps.access_token, token_file.open("w"))

    gcs_storage_client = storage.Client()
    gcs_bucket = gcs_storage_client.bucket(gcs_bucket_name)

    tables = json.load(queries_path.open("r"))
    for t in tables:
        table_name = t.get("table_name")
        projection = t.get("projection")
        queries = t.get("queries")
        print(table_name)

        # create data folder
        data_path = PROJECT_PATH / "data" / host_clean / table_name
        if not data_path.exists():
            data_path.mkdir(parents=True)
            print(f"\tCreated {'/'.join(data_path.parts[-3:])}...")

        # get table
        schema_table = ps.get_schema_table(table_name)

        # if there are queries, generate FIQL
        query_parameters = []
        if queries:
            selector = queries.get("selector")
            values = queries.get("values")
            # check if data exists for specified table
            if not [f for f in data_path.iterdir()]:
                # generate historical queries
                print("\tNo existing data. Generating historical queries...")
                query_parameters = utils.generate_historical_queries(
                    current_yearid, selector
                )
                query_parameters.reverse()
            else:
                constraint_rules = utils.get_constraint_rules(selector, current_yearid)

                # if there aren't specified values, transform yearid to value
                if not values:
                    values = [utils.transform_yearid(current_yearid, selector)]

                # for each value, get query expression
                for v in values:
                    if v == "yesterday":
                        today = datetime.date.today()
                        yesterday = today - datetime.timedelta(days=1)
                        expression = f"{selector}=ge={yesterday.isoformat()}"
                    else:
                        constraint_values = utils.get_constraint_values(
                            selector, v, constraint_rules["step_size"]
                        )
                        expression = utils.get_query_expression(
                            selector, **constraint_values
                        )
                    query_parameters.append(expression)
        else:
            query_parameters.append(None)

        for q in query_parameters:
            if q:
                print(f"\tQuerying {q} ...")
                data_file = data_path / f"{table_name}_{q}.json.gz"
            else:
                print("\tQuerying all records...")
                data_file = data_path / f"{table_name}.json.gz"

            tq_params = {"q": q}
            try:
                count = schema_table.count(**tq_params)
                print(f"\t\tFound {count} records!")
            except Exception as xc:
                print(xc)
                print(traceback.format_exc())
                continue

            if count > 0:
                if projection:
                    tq_params["projection"] = projection

                try:
                    data = schema_table.query(**tq_params)

                    # save as json.gz
                    with gzip.open(data_file, "wt", encoding="utf-8") as f:
                        json.dump(data, f)
                    print(f"\t\tSaved to {'/'.join(data_file.parts[-4:])}!")

                    # upload to GCS
                    destination_blob_name = "powerschool/" + "/".join(
                        data_file.parts[-3:]
                    )
                    blob = gcs_bucket.blob(destination_blob_name)
                    blob.upload_from_filename(data_file)
                    print(f"\t\tUploaded to {destination_blob_name}!")

                except Exception as xc:
                    print(xc)
                    print(traceback.format_exc())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("host", help="PowerSchool hostname")
    parser.add_argument("env", help=".env file")
    parser.add_argument("query", help="query file")

    args = parser.parse_args()

    try:
        main(args.host, args.env, args.query)
    except Exception as xc:
        print(xc)
        print(traceback.format_exc())
