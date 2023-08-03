from dagster import (
    asset
)


@asset
def sharing_data() -> None:
    """
    Pushes stations and vehicles published by lamassu 
    to tables in a postgis database.
    """
    print("Load assets")
