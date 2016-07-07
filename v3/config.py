import os
import re

NO_DEFAULT = object()

class Config:
    def __init__(self, kwargs):
        self.__dict__.update(kwargs)

class ConfigReader:
    def __init__(self, props):
        self.props = props

    def read(self, fn):
        print("reading {}".format(fn))
        values = dict()
        for k, v in self.props:
            values[k] = v

        values['cluster_name'] = os.path.basename(fn)

        for line in open(fn).readlines():
            m = re.match("^\\s*(?:#.*)?$", line)
            if m is not None:
                print("skipping", line)
                continue
            m = re.match("^\\s*([^= \t]+)\\s*=\\s*(\\S*)\\s*$", line)
            if m is not None:
                values[m.group(1)] = m.group(2)
                print("values={}".format(values))
                continue
            raise Exception("Could not parse {}".format(line))
        
        for k, v in values.items():
            if v is NO_DEFAULT:
                raise Exception("Required parameter missing: {}".format(k))

        return Config(values)

CONFIG_READER = ConfigReader([
    ("cluster_name", NO_DEFAULT),
    ("aws_secret_access_key", NO_DEFAULT),
    ("aws_access_key_id", NO_DEFAULT),
    ("ami_id", NO_DEFAULT),
    ("key_name", NO_DEFAULT),
    ("master_instance_type", "NO_DEFAULT"),
    ("region", "us-east")
])
