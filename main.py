"""
Main CLI or app entry point for the Births dataset analysis
"""

from mylib.lib import (
    extract_csv,
    start_spark,
    end_spark,
    load_data,
    query,
    describe,
    example_transform,
)


def process_csv_spark():
    # Download the births dataset CSV
    extract_csv(
        url="https://github.com/fivethirtyeight/data/raw/refs/heads/master/births/"
        "US_births_2000-2014_SSA.csv",
        file_path="births.csv",
        directory="data",
    )

    # Start Spark session
    sc = start_spark("BirthsDatasetAnalysis")

    if sc:
        print("Spark Session started!")
    else:
        print("Failed to start session")
        return

    # Load the dataset
    df = load_data(sc)

    # Example SQL query: Count of births by year and day of the week
    res = query(
        sc,
        df,
        query="""
            SELECT year, day_of_week, SUM(births) as total_births
            FROM Births
            GROUP BY year, day_of_week
            ORDER BY total_births DESC
        """,
        name="Births",
    )

    # Describe the dataset
    describe(df)

    # Print query result
    print(res)

    # Perform example transformations
    transform_res = example_transform(df)

    # Print transformed data
    print(transform_res)

    # End Spark session
    ending = end_spark(sc)

    if ending:
        print("Spark Session closed successfully")
    else:
        print("Failed to terminate session")

    return True


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    process_csv_spark()
