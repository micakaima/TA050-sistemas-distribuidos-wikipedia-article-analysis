import yaml

CANT_CLIENTS = 3
CANT_COLUMN_FILTERS = 2
CANT_VOCABULARY_EXTRACTORS = 2
CANT_YEAR_FILTERS = 2
CANT_ENGLISH_FILTERS = 2
CANT_BIO_FILTERS = 2
CANT_IMAGE_DOWNLOADERS = 2
CANT_YEAR_SPLITTERS = 2
CANT_OLD_ARTICLES_AVERAGERS = 2
CANT_ABOVE_AVERAGE_FILTERS = 2
CANT_SHARED_WORDS_AGGREGATORS = 2
CANT_PARTIAL_TOP_AGGREGATORS = 2

CONNECTION_ATTEMPS=10
CONNECTION_ATTEMPTS_DELAY_MS=1000
CLIENT_BATCH_SIZE=5
ABOVE_AVERAGE_FILTER_BATCH_SIZE=1000
MAX_DOWNLOAD_RETRIES=3
CLIENT_TIMEOUT=10
USER_AGENT="Wikipedia-Article-Analysis/1.0"

OUTPUT_FILENAME="docker-compose.yaml"

def generate_client_containers(docker_compose):
    for i in range(CANT_CLIENTS):
        container_name = f"client_{i}"
        depends_list = ["gateway"] + [f"column_filter_{j}" for j in range(CANT_COLUMN_FILTERS)]
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/client/Dockerfile"
            },
            "container_name": container_name,
            "depends_on": depends_list,
            "environment": [
                f"INPUT_DIR=/input/input_{i}",
                f"OUTPUT_DIR=/output/output_{i}",
                "SERVER_HOST=gateway",
                "SERVER_PORT=5678",
                f"CONNECTION_ATTEMPS={CONNECTION_ATTEMPS}",
                f"CONNECTION_ATTEMPTS_DELAY_MS={CONNECTION_ATTEMPTS_DELAY_MS}",
                f"BATCH_SIZE={CLIENT_BATCH_SIZE}"
            ],
            "volumes": [
                "./data/input:/input",
                "./data/output:/output"
            ]
        }

def generate_column_filter_containers(docker_compose):
    for i in range(CANT_COLUMN_FILTERS):
        container_name = f"column_filter_{i}"
        depends_list = [f"vocabulary_extractor_{j}" for j in range(CANT_VOCABULARY_EXTRACTORS)]
        for k in range(CANT_YEAR_FILTERS):
            depends_list.append(f"year_filter_{k}")
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/column_filter/Dockerfile",
            },
            "container_name": container_name,
            "depends_on": depends_list,
            "environment": [
                f"ID={i}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                "COLUMN_FILTER_PREFIX=raw_articles",
                f"YEAR_FILTER_AMOUNT={CANT_YEAR_FILTERS}",
                "YEAR_FILTER_PREFIX=2020_candidates",
                f"VOCABULARY_EXTRACTOR_AMOUNT={CANT_VOCABULARY_EXTRACTORS}",
                "VOCABULARY_EXTRACTOR_PREFIX=raw_abstracts",
            ]
        }

def generate_year_filter_containers(docker_compose):
    for i in range(CANT_YEAR_FILTERS):
        container_name = f"year_filter_{i}"
        depends_list = [f"english_filter_{j}" for j in range(CANT_ENGLISH_FILTERS)]
        for k in range(CANT_BIO_FILTERS):
            depends_list.append(f"bio_filter_{k}")
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/year_filter/Dockerfile",
            },
            "container_name": container_name,
            "depends_on": depends_list,
            "environment": [
                f"ID={i}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                "YEAR_FILTER_PREFIX=2020_candidates",
                f"ENGLISH_FILTER_AMOUNT={CANT_ENGLISH_FILTERS}",
                "ENGLISH_FILTER_PREFIX=en_candidates",
                f"BIO_FILTER_AMOUNT={CANT_BIO_FILTERS}",
                "BIO_FILTER_PREFIX=bio_candidates",
                "YEAR=2020",
                f"COLUMN_FILTER_AMOUNT={CANT_COLUMN_FILTERS}"
            ]
        }

def generate_english_filter_containers(docker_compose):
    for i in range(CANT_ENGLISH_FILTERS):
        container_name = f"english_filter_{i}"
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/english_filter/Dockerfile",
            },
            "container_name": container_name,
            "depends_on": [
                "q1_aggregator"
            ],
            "environment": [
                f"ID={i}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                "ENGLISH_FILTER_PREFIX=en_candidates",
                "OUTPUT_QUEUE=q1_results",
                f"YEAR_FILTER_AMOUNT={CANT_YEAR_FILTERS}"
            ]
        }

def generate_bio_filter_containers(docker_compose):
    for i in range(CANT_BIO_FILTERS):
        container_name = f"bio_filter_{i}"
        depends_list = [f"image_downloader_{j}" for j in range(CANT_IMAGE_DOWNLOADERS)]
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/bio_filter/Dockerfile",
            },
            "container_name": container_name,
            "depends_on": depends_list,
            "environment": [
                f"ID={i}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                "BIO_FILTER_PREFIX=bio_candidates",
                f"IMAGE_DOWNLOADER_AMOUNT={CANT_IMAGE_DOWNLOADERS}",
                "IMAGE_DOWNLOADER_PREFIX=biography_metadata",
                f"YEAR_FILTER_AMOUNT={CANT_YEAR_FILTERS}"
            ]
        }

def generate_image_downloader_containers(docker_compose):
    for i in range(CANT_IMAGE_DOWNLOADERS):
        container_name = f"image_downloader_{i}"
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/image_downloader/Dockerfile",
            },
            "container_name": container_name,
            "depends_on": [
                "q2_aggregator",
            ],
            "environment": [
                f"ID={i}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                "IMAGE_DOWNLOADER_PREFIX=biography_metadata",
                "OUTPUT_QUEUE=q2_results",
                f"BIO_FILTER_AMOUNT={CANT_BIO_FILTERS}",
                f"MAX_DOWNLOAD_RETRIES={MAX_DOWNLOAD_RETRIES}",
                f"CLIENT_TIMEOUT={CLIENT_TIMEOUT}",
                f"USER_AGENT={USER_AGENT}_{i}",
            ]
        }


def generate_vocabulary_extractor_containers(docker_compose):
    for i in range(CANT_VOCABULARY_EXTRACTORS):
        container_name = f"vocabulary_extractor_{i}"
        depends_list = [f"year_splitter_{j}" for j in range(CANT_YEAR_SPLITTERS)]
        for k in range(CANT_SHARED_WORDS_AGGREGATORS):
            depends_list.append(f"shared_words_aggregator_{k}")
        for l in range(CANT_PARTIAL_TOP_AGGREGATORS):
            depends_list.append(f"partial_top_aggregator_{l}")
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/vocabulary_extractor/Dockerfile",
            },
            "container_name": container_name,
            "depends_on": depends_list,
            "environment": [
                f"ID={i}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                "VOCABULARY_EXTRACTOR_PREFIX=raw_abstracts",
                "OUTPUT_QUEUE=word_count",
                f"SHARED_WORDS_AGGREGATOR_AMOUNT={CANT_SHARED_WORDS_AGGREGATORS}",
                "SHARED_WORDS_AGGREGATOR_PREFIX=shared_words_candidates",
                f"PARTIAL_TOP_AGGREGATOR_AMOUNT={CANT_PARTIAL_TOP_AGGREGATORS}",
                "PARTIAL_TOP_AGGREGATOR_PREFIX=partial_top_candidates",
                f"YEAR_SPLITTER_AMOUNT={CANT_YEAR_SPLITTERS}",
                "YEAR_SPLITTER_PREFIX=year_splitter",
                f"COLUMN_FILTER_AMOUNT={CANT_COLUMN_FILTERS}"
            ]
        }

def generate_above_average_filter_containers(docker_compose):
    for i in range(CANT_ABOVE_AVERAGE_FILTERS):
        container_name = f"above_average_filter_{i}"
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/above_average_filter/Dockerfile"
            },
            "container_name": container_name,
            "depends_on": [
                "q5_aggregator"
            ],
            "environment": [
                f"ID={i}",
                "ABOVE_AVERAGE_FILTER_PREFIX_DATA=above_average_filter_data",
                "ABOVE_AVERAGE_FILTER_PREFIX_AVERAGE=above_average_filter_average",
                f"YEAR_SPLITTER_AMOUNT={CANT_YEAR_SPLITTERS}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                "OUTPUT_QUEUE=q5_results",
                f"BATCH_SIZE={ABOVE_AVERAGE_FILTER_BATCH_SIZE}"
            ]
        }

def generate_old_articles_averager_containers(docker_compose):
    for i in range(CANT_OLD_ARTICLES_AVERAGERS):
        container_name=f"old_articles_averager_{i}"
        depends_list = [f"above_average_filter_{j}" for j in range(CANT_ABOVE_AVERAGE_FILTERS)]
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/old_articles_averager/Dockerfile"
            },
            "container_name": container_name,
            "depends_on": depends_list,
            "environment": [
                f"ID={i}",
                "OLD_ARTICLES_AVERAGER_PREFIX=old_articles_averager",
                f"ABOVE_AVERAGE_FILTER_AMOUNT={CANT_ABOVE_AVERAGE_FILTERS}",
                "ABOVE_AVERAGE_FILTER_PREFIX_AVERAGE=above_average_filter_average",
                f"YEAR_SPLITTER_AMOUNT={CANT_YEAR_SPLITTERS}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
            ]
        }

def generate_shared_words_aggregator_containers(docker_compose):
    for i in range(CANT_SHARED_WORDS_AGGREGATORS):
        container_name = f"shared_words_aggregator_{i}"
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/shared_words_aggregator/Dockerfile"
            },
            "container_name": container_name,
            "depends_on": [
                "q3_aggregator"
            ],
            "environment": [
                f"ID={i}",
                "SHARED_WORDS_AGGREGATOR_PREFIX=shared_words_candidates",
                "OUTPUT_QUEUE=q3_results",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                f"VOCABULARY_EXTRACTOR_AMOUNT={CANT_VOCABULARY_EXTRACTORS}"
            ]
        }

def generate_partial_top_aggregator_containers(docker_compose):
    for i in range(CANT_PARTIAL_TOP_AGGREGATORS):
        container_name=f"partial_top_aggregator_{i}"
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/partial_top_aggregator/Dockerfile"
            },
            "container_name": container_name,
            "depends_on": [
                "q4_aggregator"
            ],
            "environment": [
                f"ID={i}",
                "PARTIAL_TOP_AGGREGATOR_PREFIX=partial_top_candidates",
                "OUTPUT_QUEUE=final_top_candidates",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                f"VOCABULARY_EXTRACTOR_AMOUNT={CANT_VOCABULARY_EXTRACTORS}"
            ]
        }

def generate_result_aggregators(docker_compose):
    for i in range(1, 6):
        container_name = f"q{i}_aggregator"
        input_queue = f"q{i}_results" if i != 4 else "final_top_candidates"
        environment_vars = [
            "MOM_HOST=rabbitmq",
            "MOM_PORT=5672",
            f"INPUT_QUEUE={input_queue}",
            "OUTPUT_QUEUE=results",
            "STORAGE_DIR=/app/storage"
        ]
        if i == 1:
            environment_vars.append(f"ENGLISH_FILTER_AMOUNT={CANT_ENGLISH_FILTERS}")
        elif i == 2:
            environment_vars.append(f"IMAGE_DOWNLOADER_AMOUNT={CANT_IMAGE_DOWNLOADERS}")
        elif i == 3:
            environment_vars.append(f"SHARED_WORDS_AGGREGATOR_AMOUNT={CANT_SHARED_WORDS_AGGREGATORS}")
        elif i == 4:
            environment_vars.append(f"PARTIAL_TOP_AGGREGATOR_AMOUNT={CANT_PARTIAL_TOP_AGGREGATORS}")
        elif i == 5:
            environment_vars.append(f"ABOVE_AVERAGE_FILTER_AMOUNT={CANT_ABOVE_AVERAGE_FILTERS}")
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": f"src/workers/result_aggregator/q{i}_result_aggregator/Dockerfile"
            },
            "container_name": container_name,
            "depends_on": [
                "gateway"
            ],
            "volumes": [
                "./data/storage:/app/storage",
            ],
            "environment": environment_vars
        }

def generate_year_splitter_containers(docker_compose):
    for i in range(CANT_YEAR_SPLITTERS):
        container_name = f"year_splitter_{i}"        
        depends_list = [f"old_articles_averager_{j}" for j in range(CANT_OLD_ARTICLES_AVERAGERS)]
        for k in range(CANT_ABOVE_AVERAGE_FILTERS):
            depends_list.append(f"above_average_filter_{k}")
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/year_splitter/Dockerfile",
            },
            "container_name": container_name,
            "depends_on": depends_list,
            "environment": [
                f"ID={i}",
                "YEAR_SPLITTER_PREFIX=year_splitter",
                f"OLD_ARTICLES_AVERAGER_AMOUNT={CANT_OLD_ARTICLES_AVERAGERS}",
                "OLD_ARTICLES_AVERAGER_PREFIX=old_articles_averager",
                f"ABOVE_AVERAGE_FILTER_AMOUNT={CANT_ABOVE_AVERAGE_FILTERS}",
                "ABOVE_AVERAGE_FILTER_PREFIX_DATA=above_average_filter_data",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                "INPUT_QUEUE=word_count",
                f"VOCABULARY_EXTRACTOR_AMOUNT={CANT_VOCABULARY_EXTRACTORS}"
            ]
        }

def generate_gateway_container(docker_compose):
    container_name = "gateway"
    docker_compose["services"][container_name] = {
        "build": {
            "context": ".",
            "dockerfile": "src/gateway/Dockerfile",
        },
        "container_name": container_name,
        "depends_on": {
            "rabbitmq": {
                "condition": "service_healthy"
            }
        },
        "environment": [
            "SERVER_HOST=gateway",
            "SERVER_PORT=5678",
            "OUTPUT_QUEUE=results",
            "MOM_HOST=rabbitmq",
            "MOM_PORT=5672",
            f"COLUMN_FILTER_AMOUNT={CANT_COLUMN_FILTERS}",
            "COLUMN_FILTER_PREFIX=raw_articles"
        ]
    }

def generate_rabbitmq_container(docker_compose):
    container_name = "rabbitmq"
    docker_compose["services"][container_name] = {
        "build": {
            "context": "./src/rabbitmq",
            "dockerfile": "Dockerfile"
        },
        "container_name": container_name,
        "environment": [
            "RABBITMQ_LOG_LEVELS=error",
            "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=-rabbit max_message_size 536870912"
        ],
        "healthcheck": {
            "interval": "5s",
            "retries": 10,
            "start_period": "50s",
            "test": "rabbitmq-diagnostics check_port_connectivity",
            "timeout": "3s",
        },
        "ports": [
            "5672:5672",
            "15672:15672"
        ]
    }

def main():
    docker_compose = {
        "services": {}
    }

    generate_client_containers(docker_compose)
    generate_gateway_container(docker_compose)
    generate_rabbitmq_container(docker_compose)
    generate_column_filter_containers(docker_compose)
    generate_vocabulary_extractor_containers(docker_compose)
    generate_shared_words_aggregator_containers(docker_compose)
    generate_year_splitter_containers(docker_compose)
    generate_old_articles_averager_containers(docker_compose)
    generate_above_average_filter_containers(docker_compose)
    generate_year_filter_containers(docker_compose)
    generate_english_filter_containers(docker_compose)
    generate_bio_filter_containers(docker_compose)
    generate_image_downloader_containers(docker_compose)
    generate_partial_top_aggregator_containers(docker_compose)
    generate_result_aggregators(docker_compose)

    with open(OUTPUT_FILENAME, "w") as f:
        yaml.dump(docker_compose, f, indent=True, sort_keys=False)

if __name__ == "__main__":
    main()

    

