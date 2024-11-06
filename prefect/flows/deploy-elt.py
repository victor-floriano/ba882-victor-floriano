from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/victor-floriano/ba882-victor-floriano.git",
        entrypoint="prefect/flows/etl.py:etl_flow",
    ).deploy(
        name="aws-blogs-etl",
        work_pool_name="victor-pool1",
        job_variables={"env": {"BROCK": "loves-to-code"},
                       "pip_packages": ["pandas", "requests"]},
        cron="15 0 * * *",
        tags=["prod"],
        description="The pipeline to populate the stage schema with the newest posts.  Version is just for illustration",
        version="1.0.0",
    )