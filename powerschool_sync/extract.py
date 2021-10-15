import argparse
import datetime
import gzip
import json
import os
import pathlib
import traceback

from dotenv import load_dotenv
from google.cloud import storage
from powerschool import PowerSchool, utils

from datarobot.utilities import email

PROJECT_PATH = pathlib.Path(__file__).absolute().parent


def main(host, env_file, query_file):
    print(host, env_file, query_file)
    host_clean = host.replace(".", "_")

    env_filepath = PROJECT_PATH / "envs" / host_clean / env_file
    if not env_filepath.exists():
        if not env_filepath.parent.exists():
            print(f"Creating {env_filepath.parent}...")
            env_filepath.parent.mkdir(parents=True)
        raise FileNotFoundError(f"Create {env_filepath} and try again!")

    queries_filepath = PROJECT_PATH / "queries" / host_clean / query_file
    if not queries_filepath.exists():
        if not queries_filepath.parent.exists():
            queries_filepath.parent.mkdir(parents=True)
            print(f"Creating {queries_filepath.parent}...")
        raise FileNotFoundError(f"Create {queries_filepath} and try again!")

    load_dotenv(dotenv_path=env_filepath)

    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    token_filename = os.getenv("TOKEN_FILE")
    current_yearid = int(os.getenv("CURRENT_YEARID"))
    gcs_bucket_name = os.getenv("GCS_BUCKET_NAME")

    client_credentials = (client_id, client_secret)

    # load access token file, if exists
    token_filepath = PROJECT_PATH / "tokens" / host_clean / token_filename
    if not token_filepath.exists():
        print(f"{token_filepath} does not exist!")
        if not token_filepath.parent.exists():
            print(f"Creating {token_filepath.parent}...")
            token_filepath.parent.mkdir(parents=True)

        print("Fetching new access token...")
        ps = PowerSchool(host=host, auth=client_credentials)

        print(f"Saving new access token to {token_filepath}...")
        with token_filepath.open("w") as f:
            json.dump(ps.access_token, f)
    else:
        print(f"Loading {token_filepath}...")
        with token_filepath.open("r") as f:
            token_dict = json.load(f)

        now = datetime.datetime.utcnow()
        expires_at = datetime.datetime.fromtimestamp(token_dict["expires_at"])
        token_remaining = expires_at - now

        # check token validity and authenticate
        if token_remaining.days <= 2:
            print("Token expired!")
            print("Fetching new access token...")
            ps = PowerSchool(host=host, auth=client_credentials)

            print(f"Saving new access token to {token_filepath}...")
            with token_filepath.open("w") as f:
                json.dump(ps.access_token, f)
        else:
            ps = PowerSchool(host=host, auth=token_dict)

    gcs_storage_client = storage.Client()
    gcs_bucket = gcs_storage_client.bucket(gcs_bucket_name)

    with queries_filepath.open("r") as f:
        tables = json.load(f)

    for t in tables:
        table_name = t.get("table_name")
        projection = t.get("projection")
        queries = t.get("queries")
        print(f"{table_name}")

        # create data folder
        data_path = PROJECT_PATH / "data" / host_clean / table_name
        if not data_path.exists():
            data_path.mkdir(parents=True)
            print(f"\tCreated {'/'.join(data_path.parts[-3:])}...")

        # get table
        schema_table = ps.get_schema_table(table_name)

        # if there are queries, generate FIQL
        query_params = []
        if queries:
            selector = queries.get("selector")
            values = queries.get("values")

            # check if data exists for specified table
            if not [f for f in data_path.iterdir()]:
                # generate historical queries
                print("\tNo existing data. Generating historical queries...")
                query_params = utils.generate_historical_queries(
                    current_yearid, selector
                )
                query_params.reverse()
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

                    query_params.append(expression)
        else:
            query_params.append({})

        for q in query_params:
            q_params = {}
            if q:
                print(f"\tQuerying {q} ...")
                data_file = data_path / f"{table_name}_{q}.json.gz"
                q_params["q"] = q
            else:
                print("\tQuerying all records...")
                data_file = data_path / f"{table_name}.json.gz"

            try:
                count = schema_table.count(**q_params)
                print(f"\t\tFound {count} records!")
            except Exception as xc:
                print(xc)
                print(traceback.format_exc())
                email_subject = f"{host} Count Error - {table_name}"
                email_body = f"{xc}\n\n{traceback.format_exc()}"
                email.send_email(subject=email_subject, body=email_body)
                continue

            if count > 0:
                if projection:
                    q_params["projection"] = projection

                try:
                    data = schema_table.query(**q_params)

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
                    email_subject = f"{host} Extract Error - {table_name}"
                    email_body = f"{xc}\n\n{traceback.format_exc()}"
                    email.send_email(subject=email_subject, body=email_body)


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
        email_subject = f"{args.host} Extract Error - {args.query}"
        email_body = f"{xc}\n\n{traceback.format_exc()}"
        email.send_email(subject=email_subject, body=email_body)
