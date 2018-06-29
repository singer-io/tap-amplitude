#!/usr/bin/env python3

import backoff
import snowflake.connector


@backoff.on_exception(backoff.expo,
                      (snowflake.connector.Error),
                      max_tries=5,
                      factor=2)


def connect_with_backoff(config):
  return snowflake.connector.connect(user=config['username'],
    password=config['password'],
    account=config['account'],
    database=config['database'],
    warehouse=config['warehouse']
  )
