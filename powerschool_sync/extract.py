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


def main(host, env_file_name, query_file_name):
    print(host, env_file_name, query_file_name)
    host_clean = host.replace(".", "_")

    env_file_path = PROJECT_PATH / "envs" / host_clean / env_file_name
    if not env_file_path.exists():
        if not env_file_path.parent.exists():
            print(f"Creating {env_file_path.parent}...")
            env_file_path.parent.mkdir(parents=True)
        raise FileNotFoundError(f"Create {env_file_path} and try again!")

    queries_file_path = PROJECT_PATH / "queries" / host_clean / query_file_name
    if not queries_file_path.exists():
        if not queries_file_path.parent.exists():
            queries_file_path.parent.mkdir(parents=True)
            print(f"Creating {queries_file_path.parent}...")
        raise FileNotFoundError(f"Create {queries_file_path} and try again!")

    load_dotenv(dotenv_path=env_file_path)

    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    token_file_name = os.getenv("TOKEN_FILE")
    current_yearid = int(os.getenv("CURRENT_YEARID"))
    gcs_bucket_name = os.getenv("GCS_BUCKET_NAME")

    client_credentials = (client_id, client_secret)

    # load access token file and authenticate
    token_file_path = PROJECT_PATH / "tokens" / host_clean / token_file_name
    try:
        print(f"Loading {token_file_path}...")
        with token_file_path.open("rt") as f:
            token_dict = json.load(f)

        ps = PowerSchool(host=host, auth=token_dict)

        # test access token is still valid
        ps._request("GET", "/ws/schema/area")
    except:
        if not token_file_path.exists():
            print(f"\tToken does not exist!")
            if not token_file_path.parent.exists():
                print(f"Creating {token_file_path.parent}...")
                token_file_path.parent.mkdir(parents=True)
        else:
            print("Token invalid or expired!")

        print("Fetching new access token...")
        ps = PowerSchool(host=host, auth=client_credentials)

        print(f"Saving new access token to {token_file_path}...")
        with token_file_path.open("wt") as f:
            json.dump(ps.access_token, f)
            f.truncate()

    gcs_storage_client = storage.Client()
    gcs_bucket = gcs_storage_client.bucket(gcs_bucket_name)

    with queries_file_path.open("rt") as f:
        tables = json.load(f)

    for t in tables:
        table_name = t.get("table_name")
        projection = t.get("projection")
        queries = t.get("queries")
        print(table_name)

        # create data folder
        file_dir = PROJECT_PATH / "data" / host_clean / table_name
        if not file_dir.exists():
            file_dir.mkdir(parents=True)
            print(f"\tCreated {file_dir}...")

        # get table
        schema_table = ps.get_schema_table(table_name)

        # if there are queries, generate FIQL
        query_params = []
        if queries:
            selector = queries.get("selector")
            values = queries.get("values")

            # check if data exists for specified table
            if not [f for f in file_dir.iterdir()]:
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
                q_params["q"] = q
                file_name = f"{table_name}_{q}.json.gz"
            else:
                print("\tQuerying all records...")
                file_name = f"{table_name}.json.gz"
            file_path = file_dir / file_name

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
                    with gzip.open(file_path, "wt", encoding="utf-8") as f:
                        json.dump(data, f)
                    print(f"\t\tSaved to {file_path}!")

                    # upload to GCS
                    file_path_parts = file_path.parts
                    destination_blob_name = f"powerschool/{'/'.join(file_path_parts[file_path_parts.index('data') + 1:])}"
                    blob = gcs_bucket.blob(destination_blob_name)
                    blob.upload_from_filename(file_path)
                    print(f"\t\tUploaded to {blob.public_url}!")

                except Exception as xc:
                    print(xc)
                    print(traceback.format_exc())
                    email_subject = f"{host} Extract Error - {table_name}"
                    email_body = f"{xc}\n\n{traceback.format_exc()}"
                    email.send_email(subject=email_subject, body=email_body)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("host", help="PowerSchool hostname")
    parser.add_argument("env", help=".env file name")
    parser.add_argument("query", help="query file name")

    args = parser.parse_args()

    try:
        main(args.host, args.env, args.query)
    except Exception as xc:
        print(xc)
        print(traceback.format_exc())
        email_subject = f"{args.host} Extract Error - {args.query}"
        email_body = f"{xc}\n\n{traceback.format_exc()}"
        email.send_email(subject=email_subject, body=email_body)
