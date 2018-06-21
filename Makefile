
#
# Default.
#

default: build

#
# Tasks.
#

# Run test.
test:
	TAP_SNOWFLAKE_USERNAME=andyjiang TAP_SNOWFLAKE_PASSWORD="EBzvHxUQeboU;yZ4szuu" TAP_SNOWFLAKE_ACCOUNT=hh09184 TAP_SNOWFLAKE_DATABASE=test TAP_SNOWFLAKE_WAREHOUSE=COMPUTE_WH nosetests --nocapture

# Build.
build: 
	@pip3 install .

# Dev.
dev:
	@python3 setup.py develop

#
# Phonies.
#

.PHONY: test
.PHONY: build
.PHONY: dev


