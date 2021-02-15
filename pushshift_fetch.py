import requests
import boto3

def get_data(stock, start_date):
    """Fetch a maximum of the 100 most recent records for a given 
    stock starting at the start_date.

    Args:
        stock (string): Stock Ticker
        start_date (int): UNIX date time
    """

    # Build the query string
    request_url = f"https://api.pushshift.io/reddit/search/comment/?q={stock}&sort=asc&size=100&after={start_date}"

    # get the query and convert to json
    result_json = requests.get(request_url).json()

    return result_json

def save_data(data, stock):
    bucket = 'first-bucket-core-123' # already created on S3
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, 'df.csv').put(Body=data)

def get_last_date(data):
    """Get's the most recent created date in the data.

    Args:
        data (dict): JSON object that is returned from the pushshift api. 

    Returns:
        int: The most recent created date.
    """

    last_date = 0

    for entry in data["data"]:
        if entry["created_utc"] > last_date:
            last_date = entry["created_utc"]
    
    return last_date

def get_data_length(data):
    """Gets the number of entries in the data from the result of a pushshift 
    api call.

    Args:
        data (dict): JSON object that is returned from the pushshift api.

    Returns:
        int: Length of the data.
    """
    return len(data["data"])

def handler(event, context):
    stock = event["ticker"]
    start_date = event["date"]
    
    data = get_data(stock, start_date)
    
    save_data(data, stock)
    
    last_date = get_last_date(data)
    length = get_data_length(data)

    return {
        "last_date": last_date,
        "length": length
    }