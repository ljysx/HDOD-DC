# config.py
import json

import math


def initialize_configuration():
    # 读取配置文件
    with open('/tmp/pycharm_project_714/optimized/config.json', 'r') as f:
        config = json.load(f)
    config_dict = {
        'denominator': config['general'].get('denominator', 100),
        'k_nn': config['general'].get('k_nn', 10),
        'num_dims': config['general'].get('dim_expression', 2),
        'cell_num': config['general'].get('num_of_small_cells', 501),
        'domain_min': config['general'].get('domain_min', 0.0),
        'domain_max': config['general'].get('domain_max', 10001.0),
        'di_num_buckets': config['general'].get('di_num_buckets', 2),
        'maxLimitSupporting': config['general'].get('maxLimitSupporting', 5000),
        'domains': [
            config['general'].get('domain_min', 0.0),
            config['general'].get('domain_max', 10001.0)
        ],
        'smallRange':
            (config['general'].get('domain_max', 10001.0) -
             config['general'].get('domain_min', 0.0)) /
            config['general'].get('num_of_small_cells', 501)

    }

    return config_dict

# def broadcast_config(sc, config):
#     global bro_denominator, bro_knn, bro_num_dims, bro_cell_num, bro_smallRange, bro_domains, bro_di_numBuckets, bro_maxLimitSupporting
#     return {
#         'bro_denominator': sc.broadcast(config['denominator']),
#         'bro_knn': sc.broadcast(config['k_nn']),
#         'bro_num_dims' = sc.broadcast(config['num_dims'])
#         'bro_cell_num' = sc.broadcast(config['cell_num'])
#         'bro_smallRange' = sc.broadcast(config['k_nn'])
#         'bro_domains' = sc.broadcast(config['k_nn'])
#         'bro_di_numBuckets' = sc.broadcast(config['k_nn'])
#         'bro_maxLimitSupporting' = sc.broadcast(config['k_nn']),
#     }
