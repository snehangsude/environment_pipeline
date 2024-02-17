from src._gather import gather_api_data

if __name__ == "__main__":
    try:
        pipeline = dlt.pipeline(
            pipeline_name="air_quality",
            destination="bigquery",
            dataset_name="environment_data",
            progress="log"
        )

        info = pipeline.run(gather_api_data())  
        print(info)
    except Exception as e: 
        print(f"Error: {e}")
