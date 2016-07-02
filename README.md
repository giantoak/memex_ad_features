# memex_ad_features
The purpose of this repository is to calculate a number of economic metrics on MEMEX `escorts` data. It's a small data pipeline intended to take a number of extractions from scraped advertisements and spit out usable JSON data.

## Design
The different data targets are managed by the [`Makefile`.](https://github.com/giantoak/memex_ad_features/blob/master/Makefile) Input data for a given target is passed through a number of Python and R scripts, and a generated CSV is spat out.

### Input
**Currently**, the Makefile takes as input TSV-file dumps of Lattice data stored in S3 buckets, and the tsv files dumped by the different files. **Eventually** the makefile will replace these S3 dumps Lattice files stored in HDFS. Potentially, it will also take other extractions from the different teams as well.

### Output
**Currently**, the Makefile spits out a number of different CSV files. **Eventually** it will dump these files into HDFS for general consumption.

### Target Dependencies
The below dependency graph is intended to provide an overview of the makefile and chain of targets that it can create. If it doesn't render correctly, know that all is well! Just click on the "missing" icon to get to the file itself.

![Markdown Target Dependencies](https://github.com/giantoak/memex_ad_features/blob/master/makefile_graph.svg "Makefile sources and targets")

## Target Plans
We will be pairing down this version of the makefile to focus on a limited number of targets. Specifically:
* MSA_characteristics (in the `msa_characteristics.csv` file)
* MSA level ad prices
  * `ad_p50_msa`
  * `ad_mean_msa`
  * etc.
* MSA female and male wages:
  * `male_wage_p50`
  * `female_wage_p50`
  * etc.
* average incall rate by MSA
* average outcall rate by MSA
* (Generally average rate of *any* of the extracted labels that we access by MSA.)
* Ad level characteristics (in the `ad_price_ad_level.csv` file)
* average ad price price per hour (`price_per_hour` variable in data)
* Is ad priced per hour? (`1hr` variable in data)
* Number of extracted prices (possibly not included in data at this point, but good to have)
* Phone number level details (no specific file on this in the makefile, but Steve Bach has `phones.csv` and `phones_by_month.csv` which has done a lot of this - I think we’d want to just integrate that file as well). Or we could recompute some of these, which aren't that tricky to do at the right points. 
* Characteristics at the phone number level from Steve’s data:
  * Number of ads posted by this phone number in sample (`n_ads` from steve bach’s data)
  * Number of unique cities (`n_distinct_locations`)
  * How many of posted ads for this phone number were incalls, etc (`n_incall`, etc)
* `location_tree_length` - a measure of how far apart the phone number appears around the US
