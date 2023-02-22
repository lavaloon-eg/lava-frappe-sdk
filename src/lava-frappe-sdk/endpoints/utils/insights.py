from __future__ import print_function

import json
import logging
import os
import re
import sys
from logging.handlers import TimedRotatingFileHandler

from opencensus.ext.azure.log_exporter import AzureLogHandler
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.trace import config_integration
from opencensus.trace.samplers import AlwaysOnSampler, AlwaysOffSampler
from opencensus.trace.tracer import Tracer


class Configuration(object):
    def __init__(self, file_path):
        self.key = None
        # Current directory is the 'sites' directory by default
        self.storage_path = '../logs/'
        self.cloud_role = 'ERPNext'

        if os.path.exists(file_path):
            with open(file_path, 'rt') as f:
                config = json.load(f)
                if 'key' in config:
                    if is_valid_application_insights_key(config['key']):
                        self.key = config['key']
                if 'storage_path' in config:
                    self.storage_path = config['storage_path'].strip()
                if 'cloud_role' in config:
                    self.cloud_role = config['cloud_role']

def set_cloud_role(envelope, cloud_role: str):
    """Callback function for opencensus """
    envelope.tags["ai.cloud.role"] = cloud_role
    return True

class ApplicationInsights(object):
    def __init__(self):
        self.config = Configuration('application_insights.json')
        self.logger = logging.Logger(__name__, logging.INFO)
        self.logger.addHandler(logging.StreamHandler(sys.stdout))

        config_integration.trace_integrations(['requests', 'logging'])
        if self.config.key:
            self.logger.info(f'Found a valid application insights key {self.config.key}')
            exporter = AzureExporter(instrumentation_key=self.config.key, storage_path=self.config.storage_path)
            exporter.add_telemetry_processor(lambda envelope: set_cloud_role(envelope, self.config.cloud_role))
            self.tracer = Tracer(exporter=exporter, sampler=AlwaysOnSampler())
        else:
            self.logger.info('No valid application insights key found. Disabling tracing.')
            self.tracer = Tracer(sampler=AlwaysOffSampler())

    def get_logger(self, name):
        from opencensus.log import TraceLogger
        logger = logging.getLogger(name)
        if logger is not TraceLogger:
            self.logger.debug(f"Created a logger that is not a TraceLogger. Type: {type(logger)}")

        logger.setLevel(logging.INFO)
        self.ensure_rotating_file_handler(logger)

        if self.config.key:
            self.logger.debug(f'Creating logger {name} with application insights integration')
            handler = AzureLogHandler(instrumentation_key=self.config.key,
                                      storage_path=os.path.join(self.config.storage_path))
            handler.add_telemetry_processor(lambda envelope: set_cloud_role(envelope, self.config.cloud_role))
            handler.setFormatter(logging.Formatter('%(message)s'))
            logger.addHandler(handler)
        else:
            self.logger.debug(f'Creating logger {name} without Application Insights integration')

        return logger

    def ensure_rotating_file_handler(self, logger):
        # Loggers are reused. Avoid adding duplicate handlers if one already exists.
        handlers = [h for h in logger.handlers if isinstance(h, TimedRotatingFileHandler) and
                    'lava-erp' in h.baseFilename]
        if handlers:
            return

        file_handler = TimedRotatingFileHandler(os.path.join(self.config.storage_path, 'lava-erp.log'), when='d')
        file_handler.setFormatter(logging.Formatter('%(asctime)s %(traceId)s %(spanId)s %(message)s'))
        logger.addHandler(file_handler)
        logger.info(logger.handlers)
        logger.debug(
            f'Added rotating file handler with base filename "{file_handler.baseFilename}" to logger "{logger.name}"')


def is_valid_application_insights_key(key):
    return key is not None and re.match(r'.{8}-.{4}-.{4}-.{4}-.{12}', key)
