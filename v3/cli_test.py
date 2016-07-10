import config
from cli import connect_ec2
import logging

TEST_AMI='ami-fce3c696'
TEST_KEY='pliant-key'

def test_post_notification():
    c = config.CONFIG_READER.read("flock.conf")
    ec2 = connect_ec2(c)
    ec2.post_notification("test message")

import time

def test_ec2_api(tmpdir):
    logging.basicConfig(level=logging.DEBUG)
    fn = str(tmpdir)+"/conf"
    with open(fn, "wt") as fd:
        fd.write("""

""") 
    c = config.CONFIG_READER.read("flock.conf")
    ec2 = connect_ec2(c)

    sec_group_name = "test-"+str(int(time.time()))

    sg_id = ec2.create_security_group(sec_group_name, "test_ec2_api")
    sg_id_by_name = ec2.get_security_group_id(sec_group_name)
    assert sg_id == sg_id_by_name

    assert len(ec2.get_instances_within_security_group(sg_id)) == 0
    
    instance_id = ec2.run_instance("test-instance", "t2.micro", sg_id, "")

    instances = ec2.get_instances_within_security_group(sg_id)
    assert len(instances) == 1
    assert instances[0].id == instance_id

    ec2.terminate_instances([instance_id])

    ec2.delete_security_group(sg_id)

    
