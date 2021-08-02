import boto3, logging, praw, random, time, threading, zlib
from abc import abstractmethod
from config import *
from json import dumps, JSONEncoder


class DaemonBuilder(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger()
        self.name = self.__class__.__name__
        self.last_epoch = 0

    def confirm(self, item):
        self.last_epoch = item.created_utc

    def save(self, key, item):
        body = zlib.compress(str.encode(dumps(item, cls=PRAWJSONEncoder)), level=9)
        self.client.put_object(Bucket=s3_bucket_name, Key=key, Body=body)

    def is_item_ok(self, item, keys):
        for key in keys:
            found = False
            for item_key, ignore in item.__dict__.items():
                if key == item_key:
                    found = True
            if not found:
                self.logger.critical(f"{key} not found in item from {self.name}")
                return False
        return True

    def run(self):
        while True:
            time.sleep(random.randint(0, 15))
            try:
                self.client = get_boto3()
                self.reddit = get_praw()
                self.implement_run()
            except Exception as e:
                self.logger.warning(f"{self.name} caught exception {e}, restarting")
            time.sleep(random.randint(15, 105))

    @abstractmethod
    def implement_run(self):
        pass


def get_boto3():
    session = boto3.session.Session()
    return session.client(
        "s3",
        region_name=s3_region_name,
        endpoint_url=s3_endpoint_url,
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_key,
    )


def get_praw():
    return praw.Reddit(
        client_id=praw_client_id,
        client_secret=praw_client_secret,
        refresh_token=praw_refresh_token,
        user_agent="cybersecurity-modbot",
    )


class PRAWJSONEncoder(JSONEncoder):
    """Class to encode PRAW objects to JSON."""

    def default(self, obj):
        if isinstance(obj, praw.models.base.PRAWBase):
            obj_dict = {}
            for key, value in obj.__dict__.items():
                if not key.startswith("_"):
                    obj_dict[key] = value
            return obj_dict
        else:
            return JSONEncoder.default(self, obj)
