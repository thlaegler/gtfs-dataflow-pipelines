# gtfs-dataflow-pipelines

Collections of Apache Beam Pipelines to import and process GTFS static data in Google Cloud Data Flow 


## TL;DR


### Java

```
$ export PROJECT_ID=my-gcp-project
$ export GTFS_BUCKET="gs://$PROJECT_ID-opentripplanner"
$ export GTFS_PROVIDER="nz_intercity"
$ gcloud kms decrypt --plaintext-file=service-account.json --ciphertext-file=cloudbuild/$PROJECT_ID-service-account.json.enc --location=global --keyring=$PROJECT_ID-global-keyring --key=$PROJECT_ID-global-key --project=$PROJECT_ID
$ mvn clean install
```

Local:

```
$ export GTFS_PROVIDER="nz_intercity"
$ mvn compile exec:java \
	-Dexec.mainClass=com.laegler.gtfs.TestPipeline \
	-Dexec.args="--project=$PROJECT_ID --inputFile=/home/thomas/safe/dataflow/input/$GTFS_PROVIDER.zip --outputFile=/home/thomas/safe/dataflow/output/$GTFS_PROVIDER.zip"
```

Cloud:

```
$ mvn compile exec:java \
	-Pdataflow-runner
	-Dexec.mainClass=com.laegler.gtfs.TestPipeline \
	-Dexec.args="--project=$PROJECT_ID --runner=DataflowRunner --inputFile=$GTFS_BUCKET/at/20190429120000/at.zip --outputFile=$GTFS_BUCKET/output/at.zip"
```

More info: https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven

### Python

```
$ export PROJECT_ID=my-gcp-project
$ export GTFS_BUCKET="gs://$PROJECT_ID-gtfs"
$ gcloud kms decrypt --plaintext-file=service-account.json --ciphertext-file=$PROJECT_ID-service-account.json.enc --location=global --keyring=$PROJECT_ID-global-keyring --key=$PROJECT_ID-global-key --project=$PROJECT_ID
$ python --version
$ pip --version
$ pip install -U pip
($ pip install --upgrade virtualenv)
($ virtualenv /path/to/directory)
($ source /path/to/directory/bin/activate)
($ pip install --upgrade setuptools)
$ pip install apache-beam[gcp]
$ pip install beam-nuggets
($ python -m gtfs.statik.import_gtfs_zip --output test/gtfs/import/test.zip)
$ python -m gtfs.statik.import_gtfs_zip --input $GTFS_BUCKET/at/20190429120000/at.zip \
                                         --output $GTFS_BUCKET/_DATAFLOW/output \
                                         --runner DataflowRunner \
                                         --project $PROJECT_ID \
                                         --temp_location $GTFS_BUCKET/_DATAFLOW/temp
```

More info: https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python