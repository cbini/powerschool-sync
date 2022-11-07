import argparse
import datetime
import gzip
import http
import json
import os
import pathlib
import traceback

from google.cloud import storage
from powerschool import PowerSchool, utils


def main():
    host = os.getenv("HOST")
    current_yearid = int(os.getenv("CURRENT_YEARID"))

    script_dir = pathlib.Path(__file__).absolute().parent

    parser = argparse.ArgumentParser()
    parser.add_argument("query", help="query file name")
    args = parser.parse_args()

    print(host, args.query)
    host_clean = host.replace(".", "_")

    queries_file_path = script_dir / "queries" / host_clean / args.query
    if not queries_file_path.parent.exists():
        queries_file_path.parent.mkdir(parents=True)
        print(f"Creating {queries_file_path.parent}...")

    if not queries_file_path.exists():
        raise FileNotFoundError(f"Create {queries_file_path} and try again!")

    client_credentials = (os.getenv("CLIENT_ID"), os.getenv("CLIENT_SECRET"))

    # load access token file and authenticate
    token_file_path = script_dir / "tokens" / host_clean / "token.json"
    try:
        print(f"Loading {token_file_path}...")
        with token_file_path.open("rt") as f:
            token_dict = json.load(f)

        ps = PowerSchool(host=host, auth=token_dict)
    except Exception:
        if not token_file_path.exists():
            print("Token does not exist!")
            if not token_file_path.parent.exists():
                print(f"Creating {token_file_path.parent}...")
                token_file_path.parent.mkdir(parents=True)
        else:
            print(f"Token invalid or expired!\n{traceback.format_exc()}")

        print("Fetching new access token...")
        ps = PowerSchool(host=host, auth=client_credentials)

        print(f"Saving new access token to {token_file_path}...")
        with token_file_path.open("wt") as f:
            json.dump(ps.access_token, f)
            f.truncate()

    gcs_storage_client = storage.Client()
    gcs_bucket = gcs_storage_client.bucket(os.getenv("GCS_BUCKET_NAME"))

    with queries_file_path.open("rt") as f:
        tables = json.load(f)

    for t in tables:
        table_name = t.get("table_name")
        projection = t.get("projection")
        queries = t.get("queries")
        print(table_name)

        # create data folder
        file_dir = script_dir / "data" / host_clean / table_name
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
                    current_yearid=current_yearid, query_constraint_selector=selector
                )
                query_params.reverse()
            else:
                constraint_rules = utils.get_constraint_rules(
                    selector=selector, year_id=current_yearid
                )

                # if there aren't specified values, transform yearid to value
                if not values:
                    values = [
                        utils.transform_yearid(yearid=current_yearid, selector=selector)
                    ]

                # for each value, get query expression
                for v in values:
                    if v == "yesterday":
                        today = datetime.date.today()
                        yesterday = today - datetime.timedelta(days=1)
                        expression = f"{selector}=ge={yesterday.isoformat()}"
                    else:
                        constraint_values = utils.get_constraint_values(
                            selector=selector,
                            arg_value=v,
                            step_size=constraint_rules["step_size"],
                        )
                        expression = utils.get_query_expression(
                            selector=selector, **constraint_values
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

                if xc.response.status_code == 401:
                    print("Token Expired!")
                    token_file_path.unlink()
                    exit()
                else:
                    continue

            if count > 0:
                if projection:
                    q_params["projection"] = projection

                try:
                    data = schema_table.query(**q_params)
                    len_data = len(data)
                    updated_count = schema_table.count(**q_params)

                    if len_data < count:
                        updated_count = schema_table.count(**q_params)
                        if len_data < updated_count:
                            raise Exception(
                                f"Returned record count ({len_data}) is less than"
                                f"original table count ({updated_count})"
                            )

                    # save as json.gz
                    with gzip.open(file_path, "wt", encoding="utf-8") as f:
                        json.dump(data, f)
                    print(f"\t\tSaved to {file_path}!")

                    # upload to GCS
                    fpp = file_path.parts
                    destination_blob_name = (
                        f"powerschool/" f"{'/'.join(fpp[fpp.index('data') + 1:])}"
                    )
                    blob = gcs_bucket.blob(destination_blob_name)
                    blob.upload_from_filename(file_path)
                    print(f"\t\tUploaded to {blob.public_url}!")

                except http.client.RemoteDisconnected as xc:
                    print(xc)
                    print(traceback.format_exc())
                    exit()

                except Exception as xc:
                    print(xc)
                    print(traceback.format_exc())


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
        print(traceback.format_exc())
