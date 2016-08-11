# memex_ad_features (`with_luigi` branch)
The purpose of this repository is to calculate a number of economic metrics on MEMEX `escorts` data. It's a small data pipeline intended to take a number of extractions from scraped advertisements and spit out usable JSON data.

The purpose of *this branch* is to experiment with Spotify's [`luigi`](http://luigi.readthedocs.io/en/stable/) job management system. **IT IS NOT A REPLACEMENT FOR MASTER. IT SHOULD NOT BE MERGED**

`luigi` provides a fancier version of GNU [`make`](https://www.gnu.org/software/make/) that is primarily intended for managing clusters of workers and data stores, but that can be used at any scale. It demands a robust definition of task inputs and outputs, and makes it easy to set up multiple workers so that independent portions of the processing pipeline can be executed in parallel. While we *can* use `luigi` at any scale, the documents mention using it to manage petabytes of data. That seems excessive at present.

## Design
This branch snapshots the state of the repository during the MEMEX summer camp, producing features for ads at the `_id` and phone levels, and for different regions at the city and state levels. We could almost definitely reduce the number of tasks defined and push more onto particular parameters - notably `AdFeatursPerLoc` and `EntityFeaturesPerLoc` could be combined, as could `EntityFeaturesMerged` and `AdFeaturesMerged`. (We could probably combine all four, and parameterize the various settings a bit better.) Doing so exceeds the scope of the test, however.

## Concluding thoughts
Once you get used to the boilerplate changes in `luigi`, it seems pretty darn great, and there's a lot more to explore with it.

The argument against using `luigi` is that it introduces some boilerplate into the code, and some overhead, especially since we aren't keeping a server running continuously - we'd be better off leveraging a SQL database to decrease memory requirements at present than saving a bit of build time.

`luigi` will be necessary as soon as it becomes apparent that not running our builds in parallel is forcing us to run instances for significantly longer than expected. Right now, producing four output files keeps that from being too critical. As the number of metrics increases, it will become more important. The cutoff for when "important" becomes "mandatory" is, of course, quite subjective, but I think we'll have to be closer to hitting the limits of indexed relational databases first.
