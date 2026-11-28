import yaml, os, json

CONFIG_FILE_PATH = "./scripts/configs/update_docker_compose_config.json"

def load_config(config_path=CONFIG_FILE_PATH):
    if not os.path.exists(config_path):
        raise FileNotFoundError("Config file not found")

    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return flatten_dict(data)

def flatten_dict(d, result=None):
    if result is None:
        result = {}
    for k, v in d.items():
        if isinstance(v, dict):
            flatten_dict(v, result)
        else:
            result[k] = v
    return result

def generate_client_containers(docker_compose, config):
    for i in range(config["cant_clients"]):
        container_name = f"client_{i}"
        depends_list = ["load_balancer"]
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
                "LOAD_BALANCER_HOST=load_balancer",
                "LOAD_BALANCER_PORT=5678",
                f"CONNECTION_ATTEMPS={config["connection_attempts"]}",
                f"CONNECTION_ATTEMPTS_DELAY_MS={config["connection_attempts_delay_ms"]}",
                f"BATCH_SIZE={config["client_batch_size"]}"
            ],
            "volumes": [
                "./data/input:/input",
                "./data/output:/output"
            ]
        }

def generate_column_filter_containers(docker_compose, config):
    for i in range(config["cant_column_filters"]):
        container_name = f"column_filter_{i}"
        depends_list = [f"vocabulary_extractor_{j}" for j in range(config["cant_vocabulary_extractors"])]
        for k in range(config["cant_year_filters"]):
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
                f"YEAR_FILTER_AMOUNT={config["cant_year_filters"]}",
                "YEAR_FILTER_PREFIX=2020_candidates",
                f"VOCABULARY_EXTRACTOR_AMOUNT={config["cant_vocabulary_extractors"]}",
                "VOCABULARY_EXTRACTOR_PREFIX=raw_abstracts",
            ]
        }

def generate_year_filter_containers(docker_compose, config):
    for i in range(config["cant_year_filters"]):
        container_name = f"year_filter_{i}"
        depends_list = [f"english_filter_{j}" for j in range(config["cant_english_filters"])]
        for k in range(config["cant_bio_filters"]):
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
                f"ENGLISH_FILTER_AMOUNT={config["cant_english_filters"]}",
                "ENGLISH_FILTER_PREFIX=en_candidates",
                f"BIO_FILTER_AMOUNT={config["cant_bio_filters"]}",
                "BIO_FILTER_PREFIX=bio_candidates",
                "YEAR=2020",
                f"COLUMN_FILTER_AMOUNT={config["cant_column_filters"]}"
            ]
        }

def generate_english_filter_containers(docker_compose, config):
    for i in range(config["cant_english_filters"]):
        container_name = f"english_filter_{i}"
        depends_list = [f"q1_aggregator_{j}" for j in range(config["cant_q1_aggregators"])]
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/english_filter/Dockerfile",
            },
            "container_name": container_name,
            "depends_on": depends_list,
            "environment": [
                f"ID={i}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                "ENGLISH_FILTER_PREFIX=en_candidates",
                "Q1_AGGREGATOR_PREFIX=q1_results",
                f"Q1_AGGREGATOR_AMOUNT={config['cant_q1_aggregators']}",
                f"YEAR_FILTER_AMOUNT={config['cant_year_filters']}"
            ]
        }

def generate_bio_filter_containers(docker_compose, config):
    for i in range(config["cant_bio_filters"]):
        container_name = f"bio_filter_{i}"
        depends_list = [f"image_downloader_{j}" for j in range(config["cant_image_downloaders"])]
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
                f"IMAGE_DOWNLOADER_AMOUNT={config['cant_image_downloaders']}",
                "IMAGE_DOWNLOADER_PREFIX=biography_metadata",
                f"YEAR_FILTER_AMOUNT={config['cant_year_filters']}"
            ]
        }

def generate_image_downloader_containers(docker_compose, config):
    for i in range(config["cant_image_downloaders"]):
        container_name = f"image_downloader_{i}"
        depends_list = [f"q2_aggregator_{j}" for j in range(config["cant_q2_aggregators"])]
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/image_downloader/Dockerfile",
            },
            "container_name": container_name,
            "depends_on": depends_list,
            "environment": [
                f"ID={i}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                "IMAGE_DOWNLOADER_PREFIX=biography_metadata",
                "Q2_AGGREGATOR_PREFIX=q2_results",
                f"Q2_AGGREGATOR_AMOUNT={config['cant_q2_aggregators']}",
                f"BIO_FILTER_AMOUNT={config['cant_bio_filters']}",
                f"MAX_DOWNLOAD_RETRIES={config['max_download_retries']}",
                f"CLIENT_TIMEOUT={config['client_timeout']}",
                f"USER_AGENT={config['user_agent']}_{i}",
                f"TIME_LIMIT={config['time_limit']}"
            ]
        }


def generate_vocabulary_extractor_containers(docker_compose, config):
    for i in range(config["cant_vocabulary_extractors"]):
        container_name = f"vocabulary_extractor_{i}"
        depends_list = [f"year_splitter_{j}" for j in range(config["cant_year_splitters"])]
        for k in range(config["cant_shared_words_aggregators"]):
            depends_list.append(f"shared_words_aggregator_{k}")
        for l in range(config["cant_partial_top_aggregators"]):
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
                f"SHARED_WORDS_AGGREGATOR_AMOUNT={config['cant_shared_words_aggregators']}",
                "SHARED_WORDS_AGGREGATOR_PREFIX=shared_words_candidates",
                f"PARTIAL_TOP_AGGREGATOR_AMOUNT={config['cant_partial_top_aggregators']}",
                "PARTIAL_TOP_AGGREGATOR_PREFIX=partial_top_candidates",
                f"YEAR_SPLITTER_AMOUNT={config['cant_year_splitters']}",
                "YEAR_SPLITTER_PREFIX=year_splitter",
                f"COLUMN_FILTER_AMOUNT={config['cant_column_filters']}"
            ]
        }

def generate_above_average_filter_containers(docker_compose, config):
    for i in range(config["cant_above_average_filters"]):
        container_name = f"above_average_filter_{i}"
        depends_list = [f"q5_aggregator_{j}" for j in range(config["cant_q5_aggregators"])]
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/above_average_filter/Dockerfile"
            },
            "container_name": container_name,
            "depends_on": depends_list,
            "environment": [
                f"ID={i}",
                "ABOVE_AVERAGE_FILTER_PREFIX_DATA=above_average_filter_data",
                "ABOVE_AVERAGE_FILTER_PREFIX_AVERAGE=above_average_filter_average",
                f"YEAR_SPLITTER_AMOUNT={config['cant_year_splitters']}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                "Q5_AGGREGATOR_PREFIX=q5_results",
                f"Q5_AGGREGATOR_AMOUNT={config['cant_q5_aggregators']}",
                f"BATCH_SIZE={config['above_average_filter_batch_size']}"
            ]
        }

def generate_old_articles_averager_containers(docker_compose, config):
    for i in range(config["cant_old_articles_averagers"]):
        container_name=f"old_articles_averager_{i}"
        depends_list = [f"above_average_filter_{j}" for j in range(config["cant_above_average_filters"])]
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
                f"ABOVE_AVERAGE_FILTER_AMOUNT={config['cant_above_average_filters']}",
                "ABOVE_AVERAGE_FILTER_PREFIX_AVERAGE=above_average_filter_average",
                f"YEAR_SPLITTER_AMOUNT={config['cant_year_splitters']}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
            ]
        }

def generate_shared_words_aggregator_containers(docker_compose, config):
    for i in range(config["cant_shared_words_aggregators"]):
        container_name = f"shared_words_aggregator_{i}"
        depends_list = [f"q3_aggregator_{j}" for j in range(config["cant_q3_aggregators"])]
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/shared_words_aggregator/Dockerfile"
            },
            "container_name": container_name,
            "depends_on": depends_list,
            "environment": [
                f"ID={i}",
                "SHARED_WORDS_AGGREGATOR_PREFIX=shared_words_candidates",
                "Q3_AGGREGATOR_PREFIX=q3_results",
                f"Q3_AGGREGATOR_AMOUNT={config['cant_q3_aggregators']}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                f"VOCABULARY_EXTRACTOR_AMOUNT={config['cant_vocabulary_extractors']}"
            ]
        }

def generate_partial_top_aggregator_containers(docker_compose, config):
    for i in range(config["cant_partial_top_aggregators"]):
        container_name=f"partial_top_aggregator_{i}"
        depends_list = [f"q4_aggregator_{j}" for j in range(config["cant_q4_aggregators"])]
        docker_compose["services"][container_name] = {
            "build": {
                "context": ".",
                "dockerfile": "src/workers/partial_top_aggregator/Dockerfile"
            },
            "container_name": container_name,
            "depends_on": depends_list,
            "environment": [
                f"ID={i}",
                "PARTIAL_TOP_AGGREGATOR_PREFIX=partial_top_candidates",
                "Q4_AGGREGATOR_PREFIX=q4_results",
                f"Q4_AGGREGATOR_AMOUNT={config['cant_q4_aggregators']}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                f"VOCABULARY_EXTRACTOR_AMOUNT={config['cant_vocabulary_extractors']}"
            ]
        }

def generate_result_aggregators(docker_compose, config):
    for i in range(1, 6):
        cant_aggregators = config.get(f"cant_q{i}_aggregators", 1)
        for j in range(cant_aggregators):
            container_name = f"q{i}_aggregator_{j}"
            input_exchange_prefix = f"q{i}_results"
            environment_vars = [
                f"ID={j}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                f"INPUT_EXCHANGE_PREFIX={input_exchange_prefix}",
                "STORAGE_DIR=/app/storage",
                "GATEWAY_PREFIX=gateway"
            ]
            if i == 1:
                environment_vars.append(f"ENGLISH_FILTER_AMOUNT={config['cant_english_filters']}")
            elif i == 2:
                environment_vars.append(f"IMAGE_DOWNLOADER_AMOUNT={config['cant_image_downloaders']}")
            elif i == 3:
                environment_vars.append(f"SHARED_WORDS_AGGREGATOR_AMOUNT={config['cant_shared_words_aggregators']}")
            elif i == 4:
                environment_vars.append(f"PARTIAL_TOP_AGGREGATOR_AMOUNT={config['cant_partial_top_aggregators']}")
            elif i == 5:
                environment_vars.append(f"ABOVE_AVERAGE_FILTER_AMOUNT={config['cant_above_average_filters']}")
            depends_list = [f"gateway_{k}" for k in range(config["cant_gateways"])]
            docker_compose["services"][container_name] = {
                "build": {
                    "context": ".",
                    "dockerfile": f"src/workers/result_aggregator/q{i}_result_aggregator/Dockerfile"
                },
                "container_name": container_name,
                "depends_on": depends_list,
                "volumes": [
                    f"./data/storage/q{i}_{j}:/app/storage",
                ],
                "environment": environment_vars
            }

def generate_year_splitter_containers(docker_compose, config):
    for i in range(config["cant_year_splitters"]):
        container_name = f"year_splitter_{i}"        
        depends_list = [f"old_articles_averager_{j}" for j in range(config["cant_old_articles_averagers"])]
        for k in range(config["cant_above_average_filters"]):
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
                f"OLD_ARTICLES_AVERAGER_AMOUNT={config["cant_old_articles_averagers"]}",
                "OLD_ARTICLES_AVERAGER_PREFIX=old_articles_averager",
                f"ABOVE_AVERAGE_FILTER_AMOUNT={config["cant_above_average_filters"]}",
                "ABOVE_AVERAGE_FILTER_PREFIX_DATA=above_average_filter_data",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                "INPUT_QUEUE=word_count",
                f"VOCABULARY_EXTRACTOR_AMOUNT={config["cant_vocabulary_extractors"]}"
            ]
        }

def generate_gateway_containers(docker_compose, config):
    for i in range(config["cant_gateways"]):
        container_name = f"gateway_{i}"    
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
                f"ID={i}",
                f"SERVER_HOST=gateway_{i}",
                "SERVER_PORT=5679",
                "OUTPUT_QUEUE=results",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
                f"COLUMN_FILTER_AMOUNT={config["cant_column_filters"]}",
                "COLUMN_FILTER_PREFIX=raw_articles",
                "GATEWAY_PREFIX=gateway"
            ]
        }

def generate_load_balancer(docker_compose, config):
    container_name = "load_balancer"
    depends_list = [f"gateway_{j}" for j in range(config["cant_gateways"])]
    for k in range(config["cant_column_filters"]):
        depends_list.append(f"column_filter_{k}")
    docker_compose["services"][container_name] = {
        "build": {
            "context": ".",
            "dockerfile": "src/load_balancer/Dockerfile",
        },
        "container_name": container_name,
        "depends_on": depends_list,
        "environment": [
            "LOAD_BALANCER_HOST=load_balancer",
            "LOAD_BALANCER_PORT=5678",
            "BASE_SERVER_HOST=gateway",
            "BASE_SERVER_PORT=5679",
            f"GATEWAY_AMOUNT={config["cant_gateways"]}"
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
    config = load_config()
    docker_compose = {
        "services": {}
    }

    generate_client_containers(docker_compose, config)
    generate_gateway_containers(docker_compose, config)
    generate_load_balancer(docker_compose, config)
    generate_rabbitmq_container(docker_compose)
    generate_column_filter_containers(docker_compose, config)
    generate_vocabulary_extractor_containers(docker_compose, config)
    generate_shared_words_aggregator_containers(docker_compose, config)
    generate_year_splitter_containers(docker_compose, config)
    generate_old_articles_averager_containers(docker_compose, config)
    generate_above_average_filter_containers(docker_compose, config)
    generate_year_filter_containers(docker_compose, config)
    generate_english_filter_containers(docker_compose, config)
    generate_bio_filter_containers(docker_compose, config)
    generate_image_downloader_containers(docker_compose, config)
    generate_partial_top_aggregator_containers(docker_compose, config)
    generate_result_aggregators(docker_compose, config)

    with open(config["output_filename"], "w") as f:
        yaml.dump(docker_compose, f, indent=True, sort_keys=False)

if __name__ == "__main__":
    main()

    

