# Trec-Analyzer
## About
Trec-Analyzer is a tool for developing and testing solutions for [TREC](https://trec.nist.gov/) Precission Medicine track. It is implemented as a service and can be easly deployed using `docker-compose`. To be more comprehensive Trec-Analyzer has [trec_eval](https://github.com/usnistgov/trec_eval) built in.

## Features
### Supported indices
Trec-Analyzer by default supports [Elasticsearch](https://www.elastic.co/elasticsearch) and [Terrier](http://terrier.org/) using `BM25` and `DFR` strategies. For instruction on configuring indices or adding new ones chekc [Deployment](#Deployment) and [Development](#development) parts.

### Endpoints
The service exposes REST endpoints for:
* searching given text query with chosen algortihm and engine,
* searching given TREC PM topic with chosen algortihm and engine,
* performinng search and evaluation for given TREC PM topics with chosen algortihm and engine,
* performinng search and evaluation for all TREC PM topics with chosen algortihm and engine.

Endpoints are implemented in [rest.kt](trec-analyzer/trec-service/src/main/kotlin/eu/jrie/put/trec/api/rest.kt) file. They can be easly discovered using [Postman](https://www.postman.com/) collection provided in [postman](postman) directory.

## Deployment
### Environment
The system consist of `trec-service` and Elasticsearch cluster. `trec-service` has Terrier and `trec_eval` built in. The necessary configurations are:
* for `trec-service`:
  * in `docker-compose.yml`:
    *  environtment variables: `TREC_INIT_TERRIER`, `TREC_INIT_ES` and `TREC_SERVER` (explained below)
    *  link to volume with corpus
    *  link to volume with terrier index
  * in `trec-service/config/application.conf`:
    * server and indices properties
* for Elasticearch:
  * in `docker-compose.yml`:
    *  environtment variables: `thread_pool.write.queue_size` and `ES_HEAP_SIZE` (depending on machine capabilities)
    * link to volume with es index

Performance configuration is trimmed for machine with 32x CPU and 128Gb.

### Building indices
After start of the application variables `TREC_INIT_TERRIER` and `TREC_INIT_ES` are checked. If the first is set to `1`, a new terrier index will be created using data from `/corpus`. The if the second is set to `1`, a new Elasticsearch index will be created using data from `/corpus`. Thanks to linked volumes indices are persistent with container builds.

The property `init.corpusFiles` states how many files are read from corpus dir. Set it to one for reading all files. Fields `chunkSize` and `workers` for `es` and fields `workers` for `terrier` shall be set according to capabilities of the host machine.

### Running queries
After creating indices if `TREC_SERVER` is set to `1` the API will start. By default it runs on port `8001` on the host machine.

## Development
New indices can be added by extending `IndexService` with new implementations of `Repository`.

## Acknowledgements
This project was made during Application of information technologies on Poznan Univertisy of Technology under the supervision of Prof. Czesław Jędrzejek, Phd.Eng. and Jakub Dutkiewicz, M.Eng.

