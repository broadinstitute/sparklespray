import logging
import argparse
import boto.ec2
import boto.sns
from collections import namedtuple
import config
import time
import iso8601
import datetime

log = logging.getLogger(__name__)

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
        log.debug("waiting for %s to return true. (%s)", is_done_callback, is_done)
        if is_done:
            break
        time.sleep(poll_interval)
    if not is_done:
        raise Exception("Timed out waiting for {} to return True".format(is_done_callback))

def connect_ec2(config):
    return EC2(config.region, config.aws_access_key_id, config.aws_secret_access_key, config.ami_id, config.key_name, config.subnet_id, config.vpc_id, config.notification_topic)

SSH_PORT = 22

class EC2:
    def __init__(self, region, aws_access_key_id, aws_secret_access_key, ami_id, key_name, subnet_id, vpc_id, notification_topic):
        self.ec2 = boto.ec2.connect_to_region(region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        self.sns = boto.sns.connect_to_region(region)
        assert self.ec2 is not None and self.sns is not None
        self.ami_id = ami_id
        self.key_name = key_name
        self.subnet_id = subnet_id
        self.vpc_id = vpc_id
        self.notification_topic = notification_topic

    def run_instance(self, name, instance_type, security_group, user_data):
      r = self.ec2.run_instances(self.ami_id, key_name=self.key_name, instance_type=instance_type, security_group_ids=[security_group], user_data=user_data, subnet_id=self.subnet_id)
      assert len(r.instances) == 1
      instance = r.instances[0]
      self.ec2.create_tags([instance.id], dict(Name=name))
      # TODO: Poll for confirmation that tag was created
      return instance.id

    def create_security_group(self, name, description):
        assert self.ec2 is not None
        assert self._get_security_group(name=name) == None

        sg = self.ec2.create_security_group(name, description, vpc_id=self.vpc_id)
        def does_sec_group_exist():
            return self._get_security_group(id=sg.id) != None
        wait_for(does_sec_group_exist)

        sg.authorize(ip_protocol='tcp', from_port=SSH_PORT,
                             to_port=SSH_PORT, cidr_ip='0.0.0.0/0')
        sg.authorize(src_group=sg, ip_protocol='icmp', from_port=-1,
                             to_port=-1)
        sg.authorize(src_group=sg, ip_protocol='tcp', from_port=1,
                             to_port=65535)
        sg.authorize(src_group=sg, ip_protocol='udp', from_port=1,
                             to_port=65535)

        return sg.id

    def _get_security_group(self, name=None, id=None):
        try:
            if name is not None:
                sgs = self.ec2.get_all_security_groups(filters={"group-name":name})
            else:
                sgs = self.ec2.get_all_security_groups(group_ids=[id])
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
        return self._get_security_group(name=name).id

    def get_instances_within_security_group(self, security_group):        
        instances = self.ec2.get_only_instances(filters={'instance.group-id':security_group})
        def calc_age(date_str):
            d = iso8601.parse_date(date_str)
            now = datetime.datetime.now()
            #FIXME: TypeError: can't subtract offset-naive and offset-aware datetimes
            #return (d-now).total_seconds()/60.0
            return None
        return [ EC2Instance(i.id, i.private_ip_address, i.tags['Name'], calc_age(i.launch_time)) for i in instances ]

    def terminate_instances(self, instances):
        self.ec2.terminate_instances(instances)

        remaining = list(instances)
        def instances_terminated():
            while len(remaining) > 0:
                i = remaining[-1]
                inst_states = self.ec2.get_only_instances([i])
                assert len(inst_states) == 1
                inst_state = inst_states[0]
                if inst_state.state == 'terminated':
                    del remaining[-1]
                else:
                    log.debug("instance %s is %s, not terminated", i, inst_state.state)
                    return False
            return True

        wait_for(instances_terminated)

    def delete_security_group(self, security_group):
        deleted = self.ec2.delete_security_group(group_id=security_group)
        assert deleted
        def sec_group_deleted():
            return self._get_security_group(id=security_group) == None
        wait_for(sec_group_deleted)

    def post_notification(self, message):
        self.sns.publish(topic=self.notification_topic, message=message, subject="Flock Notification")

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
