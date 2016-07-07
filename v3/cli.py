import logging
import argparse
import boto.ec2
from collections import namedtuple
import config
import time

EC2Instance = namedtuple("EC2Instance", "id address name age")

NomadNode = namedtuple("NomadNode", "id address resources")
NomadJob = namedtuple("NomadJob", "id requirements node")

# merged data from EC2Instance and NomadNode.  Maybe just use a (nomad, ec2) tuple instead?  More explict.
Node = namedtuple("Node", "nomad_id instance_id address resources jobs")

class Nomad:
    def get_nodes(self):
        unimp()

    def get_jobs(self):
        unimp()

def wait_for(is_done_callback, timeout=60, poll_interval=5):
    start = time.time()
    is_done = False
    while time.time() - start < timeout:
        is_done = is_done_callback()
        if is_done:
            break
        time.sleep(poll_interval)
    if not is_done:
        raise Exception("Timed out waiting for {} to return True".format(is_done_callback))

def connect_ec2(config):
    return EC2(config.region, config.aws_access_key_id, config.aws_secret_access_key, config.ami_id, config.key_name)

class EC2:
    def __init__(self, region, aws_access_key_id, aws_secret_access_key, ami_id, key_name):
        self.ec2 = boto.ec2.connect_to_region(region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        assert self.ec2 is not None
        self.ami_id = ami_id
        self.key_name = key_name

    def run_instance(self, name, instance_type, security_group, user_data):
      r = self.ec2.run_instances(self.ami_id, key_name=self.key_name, instance_type=instance_type, security_group_ids=[security_group], user_data=user_data)
      assert len(r.instances) == 1
      instance = r.instances[0]
      self.ec2.create_tags([instance.id], dict(Name=name))
      # TODO: Poll for confirmation that tag was created
      return instance.id

    def create_security_group(self, name, description):
        assert self.ec2 is not None
        sg = self.ec2.create_security_group(name, description)
        def does_sec_group_exist():
            return self._get_security_group(sg.id) != None
        wait_for(does_sec_group_exist)
        sg.authorize(ip_protocol='tcp', from_port=ssh_port,
                             to_port=ssh_port, cidr_ip=static.WORLD_CIDRIP)
        sg.authorize(src_group=sg, ip_protocol='icmp', from_port=-1,
                             to_port=-1)
        sg.authorize(src_group=sg, ip_protocol='tcp', from_port=1,
                             to_port=65535)
        sg.authorize(src_group=sg, ip_protocol='udp', from_port=1,
                             to_port=65535)
        return sg.id

    def _get_security_group(self, name):
        try:
            sgs = self.ec2.get_all_security_groups([name])
            if len(sgs) == 1:
                return sgs[0]
            elif len(sgs) == 0:
                return None
            else:
                raise Exception("Multiple security groups named {}".format(name))
        except boto.exception.EC2ResponseError as e:
            if e.error_code == "InvalidGroup.NotFound":
                return None
            raise

    def get_security_group_id(self, name):
        return self._get_security_group(name).id

    def get_instances_within_security_group(self, security_group):        
        instances = self.ec2.get_only_instances(filters={'instance.group-id':security_group})
        return [ EC2Instance(i.id, i.private_ip_address, i.name, calc_age(i.launch_time)) ]

    def terminate_instances(self, instances):
        self.ec2.terminate_instances(instances)

    def delete_security_group(self, security_group):
        deleted = delete_security_group(name=name)
        assert deleted
        def sec_group_deleted():
            return get_security_group(name) == None
        wait_for(sec_group_deleted)

    def post_notification(self, topic_id, message):
        unimp()


# default cluster name "flock"
# try reading config "flock.config"

MASTER_CLOUDINIT = """#cloud-config
runcmd:
- [ start, nomad-server ]
final_message: "The system is finally up, after $UPTIME seconds"
"""

def _start(config, args):
    ec2 = connect_ec2(config)
    security_group = ec2.create_security_group(config.name, "flock security group")
    ec2.run_instance("master", config.ami_id, config.key_name, config.master_instance_type, security_group, MASTER_CLOUDINIT)

def _stop(config, args):
    ec2 = connect_ec2(config)
    security_group_id = ec2.get_security_group_id(config.name)
    instances = ec2.get_instances_within_security_group(security_group_id)
    ec2.terminate_instances([i.id for i in instances])
    ec2.delete_security_group(security_group_id)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', help="", default="flock")
    parser.add_argument('--verbose', dest='verbose', action='store_true')
    parser.set_defaults(func=None)

    sub = parser.add_subparsers()
    p = sub.add_parser("create")
    p.set_defaults(func=_start)

    p = sub.add_parser("destroy")
    p.set_defaults(func=_stop)

    args = parser.parse_args()
    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    root = logging.getLogger()
    hdlr = logging.StreamHandler(None)
#    hdlr.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(asctime)s %(levelname)s: %(message)s'))
    root.addHandler(hdlr)
    root.setLevel(level)

    c = config.CONFIG_READER.read(args.name)

    if args.func != None:
        args.func(c, args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

# cmds
#create
#destroy
#add count instance_type 
#rm (how to determine?  List of ids?  Probably best...)
#status -
