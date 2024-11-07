from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/victor-floriano/ba882-victor-floriano.git",
        entrypoint="prefect/flows/ml-views.py:ml_datasets",
    ).deploy(
        name="ml-datasets",
        work_pool_name="victor-pool1",
        job_variables={"env": {"ENVIRONMENT": "production"},
            "pip_packages": ["pandas", "requests"]},
        cron="20 0 * * *",
        tags=["prod"],
        description="The pipeline to create ML datasets off of the staged data.  Version is just for illustration",
        version="1.0.0",
    )