import requests


def upload_to_hdfs(
    file_path: str,
    target_path: str,
    overwrite: bool = True,
):
    url = f"http://localhost:50075/webhdfs/v1/{target_path}"

    params = {
        "op": "CREATE",
        "namenoderpcaddress": "namenode:8020",
        "createparent": "true",
        "overwrite": overwrite,
    }

    with open(file_path, "rb") as file:
        response = requests.put(url, params=params, data=file)

    if response.status_code == 201:
        print(f"Successfully uploaded file {file_path} to HDFS")
    else:
        print(f"Error while uploading file {file_path} TO HDFS: {response.status_code}")
