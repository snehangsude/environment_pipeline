import json, os, sys
import requests
import dlt

@dlt.resource(
    table_name="air_quality",
    write_disposition="merge",
    primary_key="ID"
)
def gather_air_api(limit:str="1000", offset:str="0"):

    URL = "https://data.calgary.ca/resource/g9s5-qhu5.json"
    
    while True:
        response = requests.get(URL, stream=True, params= {"$limit":limit, "$offset":offset})
        
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            break

        yield response.json()
        offset = str(int(offset)+ int(limit))
        print("Air:", offset)


@dlt.resource(
    table_name="water_quality",
    write_disposition="merge",
    primary_key="ID"
)
def gather_water_api(limit:str="1000", offset:str="0"):
    
    URL = "https://data.calgary.ca/resource/y8as-bmzj.json"

    while True:
        response = requests.get(URL, stream=True, params= {"$limit":limit, "$offset":offset})
        
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            break

        yield response.json()
        offset = str(int(offset)+ int(limit))
        print("Water:" , offset)


@dlt.source
def gather_api_data():
    return [gather_air_api, gather_water_api]


if __name__ == "__main__":
    
    pipeline = dlt.pipeline(
        pipeline_name="air_quality",
        destination="bigquery",
        dataset_name="environment_data" ,
        progress="log"
    )

    info = pipeline.run(gather_api_data())
    print(info)

