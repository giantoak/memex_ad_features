## System Variables
GO_LATTICE_S3_MIRROR = s3://giantoak.memex/lattice_data_store/flat
GO_RESULTS_S3 = s3://giantoak.memex/giantoak_econ_results
INPUT_DIR = data/input
OUTPUT_DIR = data/output

.PHONY: clean results export


## Targets
clean:
	rm -r $(INPUT_DIR)
	rm -r $(OUTPUT_DIR)

data:
	if [ ! -d $(INPUT_DIR)]; then
	mkdir $(INPUT_DIR)
	fi
	aws s3 cp --recursive $(GO_LATTICE_S3_MIRROR) $(INPUT_DIR)
	# smarter approach than bulk copying would be ls'ing to see if we need to copy
	# and then only copying new files
	# When complete, might need to pull files out of the copied directory
	# to top-level home. CONFIRM THIS

results:
	if [ ! -d $(OUTPUT_DIR)]; then
	mkdir $(OUTPUT_DIR)
	fi
	python run.py $(INPUT_DIR)/*.json.gz
	# should break up this process a bit around the original outputs
	# see the original makefile.

export:
	aws s3 cp --recursive data/output $(GO_RESULTS_S3)
