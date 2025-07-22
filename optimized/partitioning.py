# partitioning.py
import sys
from collections import defaultdict
from functools import partial
from pyspark import StorageLevel

import math
import numpy as np
from pyspark.broadcast import Broadcast
from typing import Any, Dict, List, Tuple

from optimized.utils import chi_square, output_data_info
from lof.PriorityQuene import PriorityQueue
from metricspace.Metric import L2Metric, LFMetric
from metricspace.MetricObject import MetricObject
#from Partitionplan import PartitionPlan
from optimized.CellStore import CellStore
from optimized.Partitionplan import PartitionPlan
'''
1.Generating partitionplan
'''
def compute_partition_plan(parsed_data,bro_config_dict):
    '''

    Args:
        parsed_data: 100,[(0, [83.78, 96.59]), (0, [29.4, 14.53])]
        bro_config_dict:

    Returns:partition_plan_dict: {0: [0.0, 60.0, 0.0, 60.0], 1: [60.0, 200.0, 0.0, 60.0], \
                                2: [0.0, 60.0, 60.0, 200.0], 3: [60.0, 200.0, 60.0, 200.0]}

    '''
    # 将 bro_config_dict 固定住，生成一个新的函数
    reduce_partition_with_config = partial(reduce_partition, bro_config_dict=bro_config_dict)
    partition_rdd = parsed_data.groupByKey().flatMapValues(reduce_partition_with_config).map(lambda x: x[1])
    #partition_rdd.cache()
    partition_rdd_list = partition_rdd.collect()
    # 构造字典
    partition_plan_dict = {}
    for items in partition_rdd_list:
        partition_index = items[0]
        partition_domain = items[1:]
        partition_plan_dict[partition_index] = partition_domain
    print("partition_plan_dict:",partition_plan_dict)
    return partition_rdd,partition_plan_dict
#1.1 reduce_partition
def reduce_partition(values, bro_config_dict):
    # import pydevd_pycharm
    # pydevd_pycharm.settrace('172.27.235.19', port=5678, stdoutToServer=True, stderrToServer=True, suspend=True)
    k_nn = bro_config_dict.value['k_nn']
    num_dims = bro_config_dict.value['num_dims']
    cell_num = bro_config_dict.value['cell_num']
    small_range = bro_config_dict.value['smallRange']
    domains = bro_config_dict.value['domains']
    di_numBuckets = bro_config_dict.value['di_num_buckets']
    partition_store = []
    partition_size = []
    queue_of_partition = []
    frequency = [[0] * (cell_num+1) for _ in range(num_dims)]  # 初始化频率矩阵
    map_data = defaultdict(int)  # 使用 defaultdict 代替 Hashtable

    # 1.处理 每组数据
    num_data = 0
    for one_value in values:
        num_data += 1
        new_line_list = []
        for i in range(num_dims):
            temp_data_per_dim = float(one_value[i])
            index_data_per_dim = int(math.floor(temp_data_per_dim / small_range))
            frequency[i][index_data_per_dim] += 1
            new_line_list.append(str(index_data_per_dim * small_range))  # 平移后的index再映射回实际

        new_line = ",".join(new_line_list)
        map_data[new_line] += 1
    chisquares = [0] * num_dims
    # 2.计算卡方值
    for i in range(num_dims):
        chisquares[i] = chi_square(frequency[i])
    # 排序维度,返回索引
    sorted_dim = sorted(range(len(chisquares)), key=lambda k: chisquares[k])
    # 3.Initialize first PartitionPlan and load data
    start_of_range = [domains[0]] * num_dims
    end_of_range = [domains[1]] * num_dims
    pp = PartitionPlan()
    pp.setup_partition_plan(num_dims, num_data, di_numBuckets, start_of_range, end_of_range, cell_num, small_range)
    pp.add_data_points(map_data)
    print("----------------------------Start 1st Partition---------------------------------")
    # Create partitions along the first dimension
    first_pp = pp.separate_partitions(sorted_dim[0], k_nn)  # 列表包含单个维度的各个桶划分
    count_exist_partitions = [0] * num_dims  # 记录每个维度的划分个数
    count_exist_partitions[0] = 0
    # Add partitions to the queue
    for partition in first_pp:
        if partition.get_num_data() != 0:
            queue_of_partition.append(partition)
            count_exist_partitions[0] += 1

    print("over...")
    index_for_partition_store = 0  # 记录已确定的分区，不再划分
    # 4.Process partitions for each dimension
    part_time = 10 if num_dims > 10 else num_dims
    for i in range(1, part_time):
        count_exist_partitions[i] = 0
        print(f"------------------------------Start partition for {i + 1}th dimension------------------")
        new_by_dim = sorted_dim[i]

        # Process each partition in the queue
        for j in range(count_exist_partitions[i - 1]):  # 在i-1维度上的各个桶分区继续划分
            print(f"Dealing with partition {j}")
            need_partition = queue_of_partition.pop(0)

            if need_partition.get_num_data() <= 2 * k_nn + 1:  # Using the value of k (3)
                # No need to partition further
                output_str = need_partition.get_start_and_end()
                if len(output_str) >= num_dims * 2:
                    partition_store.append([0] * (num_dims * 2 + 1))
                    sub_splits = output_str.split(",")
                    # print(sub_splits)
                    partition_store[index_for_partition_store][0] = index_for_partition_store
                    for yy in range(1, num_dims * 2 + 1):
                        partition_store[index_for_partition_store][yy] = float(sub_splits[yy - 1])
                    partition_size.append(need_partition.get_num_data())
                    index_for_partition_store += 1

            else:
                # Partition further along the dimension
                new_pps = need_partition.separate_partitions(new_by_dim, k_nn)
                for k in range(len(new_pps)):
                    if i == num_dims - 1:
                        output_str = new_pps[k].get_start_and_end()
                        if len(output_str) >= num_dims * 2:
                            partition_store.append([0] * (num_dims * 2 + 1))
                            sub_splits = output_str.split(",")
                            partition_store[index_for_partition_store][0] = index_for_partition_store
                            for yy in range(1, num_dims * 2 + 1):
                                partition_store[index_for_partition_store][yy] = float(sub_splits[yy - 1])
                            partition_size.append(new_pps[k].get_num_data())
                            index_for_partition_store += 1
                            # print("b:", index_for_partition_store)
                    else:
                        if new_pps[k].get_num_data() != 0:
                            queue_of_partition.append(new_pps[k])
                            count_exist_partitions[i] += 1
    return partition_store

'''
3.Getting partition for each record...
'''
def assign_partitions(raw_data,bro_config_dict,bro_cell_plan):
    '''

    Args:
        raw_data:
        bro_config_dict:
        bro_cell_plan:

    Returns:data_partition_rdd: [(0, [40.93, 28.59, '1']), (2, [9.98, 75.08, '2'])]

    '''
    calDataPartition_with_config = partial(calDataPartition, bro_config_dict=bro_config_dict,bro_cell_plan=bro_cell_plan)
    data_partition_rdd = raw_data.map(calDataPartition_with_config)
    print("data_partition_rdd:",data_partition_rdd.take(2))
    return data_partition_rdd
#3.1 calDataPartition
def calDataPartition(record,bro_config_dict,bro_cell_plan):
    '''
    计算每条数据对应的核心分区
    Args:
        record: 1,40.9,28.5

    Returns:(blk_id, cards+rid)
            eg:(1, [40.9,28.5,'1']).....
    '''
    num_dims = bro_config_dict.value['num_dims']
    cell_num = bro_config_dict.value['cell_num']
    small_range = bro_config_dict.value['smallRange']
    cell_store = bro_cell_plan.value  # 字典
    # split_str = record.split(',')  # 分割字符串为坐标
    split_str =record
    crds = [round(float(split_str[i + 1]),4) for i in range(num_dims)]  # 从第二个元素开始解析
    cell_id = CellStore.compute_cell_store_id_from_float(crds, num_dims, cell_num, small_range)  # 计算 cell_id
    if cell_id < 0:
        return None  # 无效cell_id
    blk_id = cell_store.get(cell_id)  # 获取块 core_partition_id,不存在返回None
    if blk_id is None or blk_id < 0:
        return None  # 无效的块 id
    rid = split_str[0]
    crds.append(rid)
    return (blk_id, crds)  # 返回 (blk_id, record)

'''
4.Calculating KNN distances and getting expand border for each partition...
'''
def expand_partitions(data_partition_rdd, bro_config_dict,partition_plan_dict) -> Any:
    '''
    对每个分区进行扩展，增加冗余边界以应对KNN计算中边界影响
    Args:
        data_partition_rdd:
        bro_config_dict:
        partition_plan_dict:

    Returns:data_info_rdd length is:1000,details is [['777', [0.96, 58.16], 0, 'F', 3.92, 0.0, 0.0,
    '350|3.92|[-5.53 59.93]|0.0|0.0,273|3.12|[-27.74  12.46]|2.8|25.57,952|2.54|[-6.26 15.16]|0.0|0.0', [None]],
            extend_partitionplan: ['0,0.0,60.0,0.0,60.0,4.32,5.68,5.84,5.63', '2,0.0,60.0,60.0,200.0,5.1,5.91,5.48,0.0']

    '''
    expand_rdd = data_partition_rdd.groupByKey().map(
        lambda x: expandPartition(x, partition_plan_dict[x[0]],bro_config_dict))  # info_of_mtcobj, partition_and_expand
    expand_rdd.cache()
    #print(expand_rdd.take(3))
    data_info_rdd = expand_rdd.flatMap(lambda x: x[0])
    data_info_rdd.persist(StorageLevel.MEMORY_AND_DISK)
    #print(f"data_info_rdd length is:{data_info_rdd.count()},details is {data_info_rdd.take(2)}")
    extend_partitionplan = expand_rdd.map(lambda x: x[1])
    extend_partitionplan.persist(StorageLevel.MEMORY_AND_DISK)
    #print("extend_partitionplan:", extend_partitionplan.take(2))

    return data_info_rdd,extend_partitionplan
#4.1 expandPartition
def expandPartition(partition_dataset, partition_domain,bro_config_dict):
    '''
        处理每个分区内的数据,计算每条数据的knn信息，如kdist等，并根据数据的扩展范围决定分区的扩展边界
        Args:
            partition_dataset: 各个分区内的数据集合(part_id:dataset)
                 eg: 1: [[40.9,28.5，'9'],[34,78,'12'])...]

        Returns:data_info, partition_and_expand[:-1]
    '''
    # import pydevd_pycharm
    # pydevd_pycharm.settrace('172.27.235.19', port=5678, stdoutToServer=True, stderrToServer=True, suspend=True)
    curPartitionId = partition_dataset[0]
    dataset = partition_dataset[1]
    # print("curPartitionId:",curPartitionId,len(dataset))
    num_dims = bro_config_dict.value['num_dims']
    k_nn =bro_config_dict.value['k_nn']
    domains = bro_config_dict.value['domains']
    # 1. 用于存储 每个分区内的MetricObject
    sorted_data = []
    for record in dataset:
        mo = MetricObject(partition_id=curPartitionId, obj=record)
        sorted_data.append(mo)
    # 2. 计算每个分区的中心点
    central_pivot = []
    for j in range(num_dims):
        central_pivot.append((partition_domain[j * 2] + partition_domain[j * 2 + 1]) / 2.0)
    # print(f"partition_id:{curPartitionId},central_pivot:{central_pivot}")
    # 3. 计算分区内各个数据与中心点的距离
    metric = L2Metric()
    for metric_object in sorted_data:
        metric_object.set_dist_to_pivot(metric.dist(central_pivot, metric_object.get_obj()[:-1]))
    # 4. 降序排序
    sorted_data = sorted(sorted_data, key=lambda obj: obj.get_dist_to_pivot(), reverse=True)
    # 5. Set partition expand distance by points in that partition
    partition_expand = [0.0] * (num_dims * 2)
    can_calculate_kdist = {}
    metric = LFMetric()
    for i in range(len(sorted_data)):
        o_R = sorted_data[i]
        o_R = find_KNN_for_single_object(o_R, i, sorted_data, metric, k_nn)
        # Bound supporting area,use kdist
        current_bound = bound_supporting_area(o_R, num_dims, domains, partition_domain, can_calculate_kdist)
        # 'F' indicates expanded
        if o_R.get_type() == 'F':
            partition_expand = max_of_two_float_arrays(partition_expand, current_bound, num_dims)
    # calculate LRD for those with exact KNNs
    can_calculate_lrd = {}  # type: dict[int, MetricObject]
    # compute_lrd(can_calculate_kdist, can_calculate_lrd)
    compute_lcf(can_calculate_kdist, can_calculate_lrd)
    # calculate LOF for those that can calculate LRD
    can_calculate_nof = {}  # type: dict[int, MetricObject]
    compute_nof(can_calculate_lrd, can_calculate_nof,bro_config_dict)
    data_info = output_data_info(can_calculate_lrd, sorted_data)

    partition_and_expand = ""
    partition_and_expand += f"{curPartitionId},"
    for i in range(num_dims * 2):
        partition_and_expand += f"{partition_domain[i]},"

    # 拼接 partitionExpand 数据
    for i in range(num_dims * 2):
        partition_and_expand += f"{partition_expand[i]},"
    # print(partition_and_expand)
    # print(info_of_mtcobj)
    return data_info, partition_and_expand[:-1]
#4.1.1 find_KNN_for_single_object
def find_KNN_for_single_object(o_R, current_index, sorted_data, metric, k):
    dist = None
    pq = PriorityQueue()  # 优先队列，用来存储 K 个最近邻
    theta = float('inf')  # 阈值，表示最远的距离
    kNNfound = False
    inc_current = current_index + 1
    dec_current = current_index - 1
    i, j = 0, 0  # i---increase, j---decrease

    while not kNNfound and (inc_current < len(sorted_data) or dec_current >= 0):
        if inc_current > len(sorted_data) - 1 and dec_current < 0:
            break

        if inc_current > len(sorted_data) - 1:
            i = float('inf')
        if dec_current < 0:
            j = float('inf')

        if i <= j:
            o_S = sorted_data[inc_current]
            dist,cf = metric.dist(o_R.get_obj()[:-1], o_S.get_obj()[:-1])
            if pq.size() < k:
                pq.insert(o_S.get_obj()[-1], [dist, cf])
                theta = pq.get_priority()
            elif dist < theta:
                pq.pop()
                pq.insert(o_S.get_obj()[-1], [dist, cf])
                theta = pq.get_priority()
            inc_current += 1
            i = abs(o_R.get_dist_to_pivot() - o_S.get_dist_to_pivot())
        else:
            o_S = sorted_data[dec_current]
            dist,cf = metric.dist(o_R.get_obj()[:-1], o_S.get_obj()[:-1])
            if pq.size() < k:
                pq.insert(o_S.get_obj()[-1], [dist, cf])
                theta = pq.get_priority()
            elif dist < theta:
                pq.pop()
                pq.insert(o_S.get_obj()[-1], [dist, cf])
                theta = pq.get_priority()
            dec_current -= 1
            j = abs(o_R.get_dist_to_pivot() - o_S.get_dist_to_pivot())

        if i > pq.get_priority() and j > pq.get_priority() and pq.size() == k:
            kNNfound = True

    o_R.set_kdist(pq.get_priority())

    # 存储 KNN 详情
    knn_in_detail = o_R.get_knn_in_detail()  # 字典
    while pq.size() > 0:
        dist, id, dist_list = pq.pop()
        knn_in_detail[id] = dist_list

    return o_R
# 4.1.2 bound_supporting_area
def bound_supporting_area(o_R, num_dims, domains, partition_domain, can_calculate_kdist):
    # 初始化一个新数组，大小为 num_dims * 2
    bound_for_cur_point = [0.0] * (num_dims * 2)
    # 获取当前点对象及其坐标
    current_point = o_R.get_obj()[:-1]  # 获取对象,[27.08, 63.59, '779']
    current_kdist = o_R.get_kdist()  # 获取 K 距离
    # 标志变量
    tag = True
    # 遍历每个维度
    for i in range(num_dims):
        # 计算当前维度的最小和最大值
        min_current = max(domains[0], current_point[i] - current_kdist)
        max_current = min(domains[1] - sys.float_info.min, current_point[i] + current_kdist)
        if min_current < partition_domain[2 * i]:
            tag = False
            bound_for_cur_point[2 * i] = round(max(bound_for_cur_point[2 * i],
                                                   abs(partition_domain[2 * i] - min_current)), 2)

        # 比较最大值
        if max_current > partition_domain[2 * i + 1]:
            tag = False
            bound_for_cur_point[2 * i + 1] = round(max(bound_for_cur_point[2 * i + 1],
                                                       abs(partition_domain[2 * i + 1] - max_current)), 2)
        elif max_current == partition_domain[2 * i + 1]:
            tag = False
            bound_for_cur_point[2 * i + 1] = round(max(sys.float_info.min, bound_for_cur_point[2 * i + 1]), 2)

    # 根据tag值设置 MetricObject 的类型
    if not tag:
        o_R.set_type('F')
    else:
        o_R.set_type('T')
        can_calculate_kdist[o_R.get_obj()[-1]] = o_R

    return bound_for_cur_point
   #4.1.2.1 max_of_two_float_arrays
def max_of_two_float_arrays(partition_expand, current_bound, num_dims):
    new_array = [0] * (num_dims * 2)
    for i in range(len(partition_expand)):
        new_array[i] = max(partition_expand[i], current_bound[i])
    return new_array
#4.1.3 compute_lcf
def compute_lcf(can_calculate_kdist, can_calculate_lrd):
    # import pydevd_pycharm
    # pydevd_pycharm.settrace('172.27.235.19', port=5678, stdoutToServer=True, stderrToServer=True, suspend=True)

    for o_S in can_calculate_kdist.values():
        lcf = (0.0,0.0)
        for key_map, temp_dist in o_S.get_knn_in_detail().items():
            cf = temp_dist[1]
            lcf += cf

        lrd_core = np.round(np.linalg.norm(lcf, keepdims=True),2)
        o_S.set_lrd(lrd_core[0])
        o_S.set_type('L')
        can_calculate_lrd[o_S.get_obj()[-1]] = o_S
#4.1.4 compute_nof
def compute_nof(can_calculate_lrd, can_calculate_nof,bro_config_dict):
    knn = bro_config_dict.value['k_nn']
    for o_S in can_calculate_lrd.values():
        can_lof = True
        lof_core = 0.0

        if o_S.get_lrd() <= 1e-9:
            lof_core = 0.0
        else:
            for key_map, _ in o_S.get_knn_in_detail().items():
                if key_map in can_calculate_lrd:
                    temp_lrd = can_calculate_lrd[key_map].get_lrd()
                    if temp_lrd == 0:
                        pass  # lof_core 不变
                    else:
                        lof_core += temp_lrd
                else:
                    can_lof = False
                    break
            if can_lof:
                lof_core = round((lof_core / knn) * 1.0, 4)

        if math.isnan(lof_core) or math.isinf(lof_core):
            lof_core = 0.0

        if can_lof:
            o_S.set_lof(lof_core)
            o_S.set_type('O')
            can_calculate_nof[o_S.get_obj()[-1]] = o_S  # 推测补全

'''
5.Getting support_partition for each cell...
'''
def assign_support_partitions(extend_partitionplan,bro_config_dict,countExceedPartitions):
    '''

    Args:
        extend_partitionplan:
        bro_config_dict:
        countExceedPartitions:

    Returns:support_cell_plan_rdd: [(10, [None, 0, 2, 1]), (12, [None, 2, 3])]

    '''
    dealEachPartition_Support_with_config = partial(dealEachPartition_Support, bro_config_dict=bro_config_dict,countExceedPartitions=countExceedPartitions)
    support_cell_plan_rdd = extend_partitionplan.flatMap(dealEachPartition_Support_with_config).reduceByKey(
        lambda x, y: list(set(x + y)))
    support_cell_plan = support_cell_plan_rdd.collect()
    print("support_cell_plan_rdd:", support_cell_plan[:2])  # [(10, [None, 0, 1]), (20, [None, 0, 1])]
    cell_support_dict = {}
    for item in support_cell_plan:
        key = item[0]
        value = item[1]
        cell_support_dict[key] = value
    return cell_support_dict
#5.1 dealEachPartition_Support
def dealEachPartition_Support(partition,bro_config_dict,countExceedPartitions):
    '''
            处理每个分区，设置该分区扩展单元格内每个单元格的服务分区ID
            Args:
                partition: ['1, 20.0, 100.0, 0.0, 100.0,0.0,25.23,20.0,100.0']

            Returns:support_cell_set: [(0, [None, 3, 3]), (1, [None, 3]), (2, [None, 3]), (3, [None, 3, 3])

            '''
    partition = partition.split(",")
    index_partition = int(partition[0])
    value_partition = [float(x) for x in partition[1:]]
    num_dims = bro_config_dict.value['num_dims']
    small_range = bro_config_dict.value['smallRange']
    cell_num = bro_config_dict.value['cell_num']

    # 计算原分区中每个维度的单元格索引范围
    indexes = [0] * (num_dims * 2)
    multiple = 1
    for i in range(0, num_dims * 2, 2):
        indexes[i] = int(value_partition[i] / small_range)
        indexes[i + 1] = int(value_partition[i + 1] / small_range)
        if indexes[i] == indexes[i + 1]:
            print(f"Cannot interpret this partition, contains nothing: {index_partition}")
            return
        multiple *= (indexes[i + 1] - indexes[i])
    # Check if partition size exceeds max limit
    flagExceed = False
    maxLimitSupporting = 0
    for i in range(num_dims * 2, num_dims * 4):
        if value_partition[i] > maxLimitSupporting:
            # print(f"Exceed max limit: {value_partition[i]}")
            flagExceed = True
            # value_partition[i] = maxLimitSupporting
    # P[expand]>0,超出
    supportCellsSize = [0] * (num_dims * 2)  # size of the supporting cells
    support_cell_set = []  # eg:(cell_id),[support_partition]-[(10, [None, 0])), ...]
    if flagExceed:
        countExceedPartitions.add(1)  # Assuming this is used elsewhere
        # Step 1: Calculate the size of the supporting cells
        # for i in range(num_dims * 2):
        #     supportCellsSize[i] = math.ceil(value_partition[i + num_dims * 2] / small_range)
        # Step 2: Update the new indexes based on support size
        newindexes = [0] * (num_dims * 2)
        for i in range(num_dims):
            # newindexes[2 * i] = max(0, indexes[2 * i] - supportCellsSize[2 * i])
            # newindexes[2 * i + 1] = min(cell_num, indexes[2 * i + 1] + supportCellsSize[2 * i + 1])
            newindexes[2 * i] = max(0, int((value_partition[2 * i] - value_partition[
                2 * i + num_dims * 2]) / small_range))
            newindexes[2 * i + 1] = min(cell_num, math.ceil(
                (value_partition[2 * i + 1] + value_partition[2 * i + 1 + num_dims * 2]) / small_range))
        # indexes =[2,9,2,7]
        # newindexes = [1,9,2,9]
        # Step 3: Generate the final support list
        # finalSupportList =["0,1,","0,2,"]#test
        finalSupportList = []  # test
        new_list = []
        previous_list = []
        # import pydevd_pycharm
        # pydevd_pycharm.settrace('172.27.66.200', port=5678, stdoutToServer=True, stderrToServer=True)
        for i in range(num_dims):
            previous_list.clear()
            new_list.clear()
            if indexes[2 * i] != newindexes[2 * i]:
                for j in range(num_dims):
                    previous_list.extend(new_list)
                    new_list.clear()
                    if i == j:
                        beginIndex = newindexes[2 * i]
                        endIndex = indexes[2 * i]
                    else:
                        beginIndex = newindexes[2 * j]
                        endIndex = newindexes[2 * j + 1]

                    for k in range(beginIndex, endIndex):
                        if not previous_list:
                            new_list.append(f"{k},")
                        else:
                            for mm in range(len(previous_list)):
                                new_list.append(f"{previous_list[mm]}{k},")

                    previous_list.clear()
                finalSupportList.extend(new_list)

            previous_list.clear()
            new_list.clear()
            if indexes[2 * i + 1] != newindexes[2 * i + 1]:
                for j in range(num_dims):
                    previous_list.extend(new_list)
                    new_list.clear()
                    if i == j:
                        beginIndex = indexes[2 * i + 1]
                        endIndex = newindexes[2 * i + 1]
                    else:
                        beginIndex = newindexes[2 * j]
                        endIndex = newindexes[2 * j + 1]

                    for k in range(beginIndex, endIndex):
                        if not previous_list:
                            new_list.append(f"{k},")
                        else:
                            for mm in range(len(previous_list)):
                                new_list.append(f"{previous_list[mm]}{k},")

                    previous_list.clear()
                finalSupportList.extend(new_list)
        # print("finalSupportList", finalSupportList)
        # Step 4: Update the cell store with the support partition ID
        support_partition_store = [[None] for _ in range(int(cell_num ** num_dims))]
        for item in finalSupportList:
            cellId = CellStore.compute_cell_store_id_from_string(item[:-1], num_dims, cell_num)
            support_partition_store[cellId].append(index_partition)
            support_cell_set.append((cellId, support_partition_store[cellId]))
        print("support_cell_set:", support_cell_set)
    return support_cell_set  # eg:(cell_id),[support_partition]-[(10, [None, 0])), ...]


