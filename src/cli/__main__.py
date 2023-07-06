from src.trains.collector import _get_trains, write_dict_list_to_file
from src.trains.db import _init_db, collect_files, copy_ndjson_to_table
from src.trains.files import upload_to_hdfs
import click
import os
import shutil
from src.paths import DATA_DIR


@click.group()
def cli():
    pass


@cli.command()
def get_trains():
    trains = _get_trains()
    print(trains)


@cli.command()
def init_db():
    _init_db()


@cli.command()
def trains_pipeline():
    shutil.rmtree(DATA_DIR, ignore_errors=True)
    os.mkdir(DATA_DIR)
    trains = _get_trains()
    write_dict_list_to_file(trains, os.path.join(DATA_DIR, "trains"), nd=True)
    train_data_files = collect_files("data/trains", "json")
    for file in train_data_files:
        copy_ndjson_to_table(
            file, "trains", "trains_info", "src/trains/sql/etl/trains_info_etl.sql"
        )
        upload_to_hdfs(
            file_path=file, target_path=file.replace("\\", "/").replace("data/", "")
        )


if __name__ == "__main__":
    cli()
