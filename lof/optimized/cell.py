from functools import partial

import math

from optimized.CellStore import CellStore

def compute_index(val, small_range):
    return math.floor(val / small_range) if val < 0 else int(val / small_range)
#2.1dealEachPartition
def dealEachPartition(partition,bro_config_dict):
    '''
    处理每个分区，设置该分区单元格内每个单元格的核心ID
    Args:
        partition: [1, 20.0, 100.0, 0.0, 100.0]

    Returns:(cellid,partition_id)
        eg:[(0, 0), (10, 0), (1, 0), (11, 0), (2, 0),...]

    '''
    index_partition = partition[0]
    value_partition = partition[1:]
    # 初始化索引数组
    indexes = [0] * (bro_config_dict.value['num_dims'] * 2)
    multiple = 1
    # 计算每个维度的索引范围
    for i in range(0, bro_config_dict.value['num_dims'] * 2, 2):
        indexes[i] = compute_index(value_partition[i], bro_config_dict.value['smallRange'])
        indexes[i + 1] = compute_index(value_partition[i + 1], bro_config_dict.value['smallRange'])
        if indexes[i] == indexes[i + 1]:
            print(f"Cannot interpret this partition, contains nothing: {index_partition}")
            return

        multiple *= (indexes[i + 1] - indexes[i])

    new_list = []
    # 生成cell单元的标识符
    for i in range(bro_config_dict.value['num_dims']):
        previous_list = new_list.copy()
        new_list.clear()

        begin_index = indexes[2 * i]
        end_index = indexes[2 * i + 1]

        for j in range(begin_index, end_index):
            if not previous_list:
                new_list.append(f"{j},")
            else:
                for item in previous_list:
                    new_list.append(f"{item}{j},")
        previous_list.clear()
    cell_set = []
    for i in range(len(new_list)):
        cell_id = CellStore.compute_cell_store_id_from_string(new_list[i][:-1], bro_config_dict.value['num_dims'],
                                                              bro_config_dict.value['cell_num'])
        # cell_id = new_list[i][:-1]
        cell_set.append((cell_id, index_partition))
    return cell_set
'''
2.Getting partition for each cell...
'''
def compute_cell_plan(partition_rdd,bro_config_dict):
    '''

    Args:
        partition_rdd: [1, 20.0, 100.0, 0.0, 100.0]
        bro_config_dict:

    Returns:core_cell_rdd: [(0, 0), (9, 1), (18, 1), (1, 2), (2, 2), (10, 3), (19, 3), (11, 3), (20, 3)]
            cell_plan_dict {0: 0, 9: 1, 18: 1, 1: 2, 2: 2, 10: 3, 19: 3, 11: 3, 20: 3}

    '''
    dealEachPartition_with_config = partial(dealEachPartition, bro_config_dict=bro_config_dict)
    core_cell_rdd = partition_rdd.flatMap(dealEachPartition_with_config)
    core_cell_rdd.cache()
    cell_plan_rdd_list = core_cell_rdd.collect()
    # print("core_cell_rdd:",cell_plan_rdd_list)
    # 构造字典，key：cell_id;value:core_id_partition
    cell_plan_dict = {}
    for item in cell_plan_rdd_list:
        key = item[0]  # 拆分为键和值
        value = item[1]
        cell_plan_dict[key] = value
    print("cell_plan_dict",cell_plan_dict)
    return core_cell_rdd,cell_plan_dict

