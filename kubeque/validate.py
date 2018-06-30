import logging
import kubeque

log = logging.getLogger(__name__)

def validate_cmd(jq, io, cluster, config):
    log.info("Validating config, using kubeque %s", kubeque.__version__)
    log.info("Printing config:")
    import pprint
    pprint.pprint(config)

    log.info("Verifying we can access google cloud storage")
    sample_value = new_job_id()
    sample_url = io.write_str_to_cas(sample_value)
    fetched_value = io.get_as_str(sample_url)
    assert sample_value == fetched_value

    log.info("Verifying we can read/write from the google datastore service and google pubsub")
    jq.test_datastore_api(sample_value)

    log.info("Verifying we can access google genomics apis")
    cluster.test_api()

    log.info("Verifying google genomics can launch image \"%s\"", config['default_image'])
    logging_url = config["default_url_prefix"] + "/node-logs"
    cluster.test_image(config['default_image'], sample_url, logging_url)

    log.info("Verification successful!")
