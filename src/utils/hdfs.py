import os
import pickle
import urllib
from hdfs import InsecureClient

from config import config

class FileHandler:
    pass

class HdfsFileHandler(FileHandler):
    def __init__(self):
        self.host = config['hdfs_host']
        self.user = config['hdfs_user']
        self.client = InsecureClient(self.host, user=self.user)

    def list_dir(self, path):
        path_url = urllib.parse.quote(path)
        return [f"{path}/{p}" for p in self.client.list(path_url, status=False)]
    
    def load(self, path, encoding="utf-8"):
        with self.client.read(path, encoding=encoding) as f:
            contents = f.read()
        return contents

    def load_line(self, path, encoding="utf-8"):
        with self.client.read(path, encoding=encoding) as f:
            while True:
                contents = f.readline()
                if contents == '':
                    break
                yield contents


    def load_by_user(self, path):
        """
        :param: path    the path which has the prefix as user root hdfs path(/user/[HDFS_USER_NAME])
                        e.g., 'path' from /user/[HDFS_USER_NAME]/'path'
        """
        with self.client.read(path, encoding="utf-8") as f:
            contents = f.read()
        return contents
    
    def load_pickle(self, path):
        with self.client.read(path) as reader:
            bt_contents = reader.read()
            contents = pickle.load(bt_contents)
        return contents

    def loads_pickle(self, path):
        with self.client.read(path) as f:
            contents = f.read()
        pkl_obj = pickle.loads(contents)
        return pkl_obj

    def dumps_pickle(self, path, obj):
        contents = pickle.dumps(obj)
        self.client.write(path, data=contents)
    
    def mkdirs(self, path):
        self.client.makedirs(path)

    def write(self, path, contents, encoding='utf-8', append=False):
        self.client.write(path, data=contents, encoding=encoding, append=append)

    def exist(self, path):
        return self.client.status(path, strict=False)

    def upload(self, source, dest, overwrite=False):
        if os.path.exists(source):
            if not self.exist(dest):
                self.client.makedirs(dest)
            self.client.upload(hdfs_path=dest, local_path=source, overwrite=overwrite)
            print(f'{source} -> {dest} Uploaded')
        else:
            print(f"Source Not Exist Error: {source}")
            raise FileNotFoundError

    def download(self, source, dest):
        if self.exist(source):
            if not os.path.exists(dest):
                os.mkdir(dest)
            self.client.download(hdfs_path=source, local_path=dest, overwrite=True)
            print(f'{source} -> {dest} Downloaded')
        else:
            print(f"Source Not Exist Error: {source}")
            raise FileNotFoundError