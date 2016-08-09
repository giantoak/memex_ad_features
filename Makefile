## Force shell to be bash
SHELL := /bin/bash


## System Variables
GO_LATTICE_S3_MIRROR = s3://giantoak.memex/lattice_data_store/flat
GO_RESULTS_S3 = s3://giantoak.memex/giantoak_econ_results
INPUT_DIR = data/input
LATTICE_INPUT_SUBDIR = $(INPUT_DIR)/lattice
OUTPUT_DIR = data/output


## Targets
clean:
    for fpath in $(INPUT_DIR) $(OUTPUT_DIR); do
    if [ -d $fpath ]; then rm -r $fpath; fi
    done

data:
	for fpath in $(INPUT_DIR) $(LATTICE_INPUT_SUBDIR); do
	if [ ! -d $fpath ]; then mkdir $fpath; fi
	done
	aws s3 cp --recursive $(GO_LATTICE_S3_MIRROR) $(LATTICE_INPUT_SUBDIR)
	# smarter approach than bulk copying would be ls'ing
	# and diff'ing to see if there are new files
	# and then only copying those

results:
	if [ ! -d $(OUTPUT_DIR) ]; then mkdir $(OUTPUT_DIR); fi
	python run.py $(LATTICE_INPUT_SUBDIR)/*.json.gz

export:
	aws s3 cp --recursive $(OUTPUT_DIR) $(GO_RESULTS_S3)
