# 下面是 `knn_lrd_lof.py` 的初始模块化实现，包含以下功能组件：
#
# - `compute_knn`: 计算每个点的 K 个最近邻。
# - `compute_lrd`: 计算每个点的局部可达密度（Local Reachability Density）。
# - `compute_lof`: 计算每个点的局部离群因子（Local Outlier Factor）。
#
# ```python
# knn_lrd_lof.py
import ast
from functools import partial
from typing import List, Dict, Tuple

import math
import numpy as np
from pyspark import RDD

from lof.PriorityQuene import PriorityQueue
from metricspace.Metric import L2Metric, LFMetric
from metricspace.MetricObject import MetricObject
from optimized.CellStore import CellStore
from optimized.utils import output_data_info
'''
6. second cal knn
'''
def compute_second_knn(data_info_rdd,bro_config_dict,bro_cell_support ):
    '''

    Args:
        data_info_rdd: [['777', [0.96, 58.16], 0, 'F', 3.92, 0.0, 0.0, '350|3.92|[-5.53 59.93]|0.0|0.0,273|3.12|[-27.74  12.46]|2.8|25.57,952|2.54|[-6.26 15.16]|0.0|0.0', [None]],
              state 'F/L/O'
        bro_config_dict:
        bro_cell_support:

    Returns:knn_dist_second_rdd:234,details is [['777', [0.96, 58.16], 0, 'L', 3.55, 29.36, 0.0,
    '153|3.55|[  9.45 -43.73]|0.0|0.0,273|3.12|[-27.74  12.46]|0.0|0.0,952|2.54|[-6.26 15.16]|0.0|0.0', [None, 1, 2, 3]],
    both state 'L/O'
    '''
    print("6.1 Cal support partition for each data...")
    calData_SupportPartition_with_config = partial(calData_SupportPartition, bro_config_dict=bro_config_dict,bro_cell_support=bro_cell_support)
    support_data_plan_rdd = data_info_rdd.map(calData_SupportPartition_with_config)
    print(f"support_data_plan_rdd is:{support_data_plan_rdd.count()},details is {support_data_plan_rdd.take(2)}")  # [([[1, '777', 0.96, 58.16, 'S', 'F', 3.92, 0.0], [2, '777', 0.96, 58.16, 'S', 'F', 3.92, 0.0],
    support_kdist_info_rdd = (support_data_plan_rdd.filter(lambda x: x[0]).flatMap(lambda x: x[0]).map(lambda y: (y[0], (y[1], y[2], y[3], y[4], y[5], y[6], y[7]))))
    core_first_unfinished_rdd = support_data_plan_rdd.map(
        lambda x: (x[1][0], (x[1][1], x[1][2], x[1][3], x[1][4], x[1][5], x[1][6], x[1][7], x[1][8], x[1][9]))).filter(
        lambda x: x[1][3] != 'O')
    print(f"support_kdist_info_rdd is:{support_kdist_info_rdd.count()},details is {support_kdist_info_rdd.take(2)}")  # 哪些点支持该分区(1, (711, 12.38, 1.32, 'S', 'O', 6.67, 0.1748))...
    print(f"core_first_unfinished_rdd is:{core_first_unfinished_rdd.count()},details is {core_first_unfinished_rdd.take(2)}")  # 核心分区内的数据[(0, ('777', [0.96, 58.16], 'C', 'F', 3.92, 0.0, 0.0, '350|3.92|[-5.53 59.93]|0.0|0.0,273|3.12|[-27.74  12.46]|2.8|25.57,952|2.54|[-6.26 15.16]|0.0|0.0', [None, 1, 2, 3])),
    print("6.2. Cal knn for expand data...")
    partition_details_rdd = support_kdist_info_rdd.union(core_first_unfinished_rdd).groupByKey()
    # b).计算所有点的knn(包含扩展点)
    # ['925,[5.68, 0.67],0,6.51,[None],549|6.51,74|1.65,795|1.52',...]
    cal_final_knn_with_config = partial(cal_final_knn, bro_config_dict=bro_config_dict)
    knn_dist_second_rdd = partition_details_rdd.flatMap(cal_final_knn_with_config)  # sec_data_info
    knn_dist_second_rdd.cache()
    print(f"knn_dist_second_rdd:{knn_dist_second_rdd.count()},details is {knn_dist_second_rdd.take(2)}") #L/O
    return support_data_plan_rdd,knn_dist_second_rdd
#6.1 calData_SupportPartition
def calData_SupportPartition(knn_info,bro_config_dict,bro_cell_support):
    '''
    根据cell-support_plan,确定每条数据的支持分区
    Args:
        knn_info: '908,[10.96, 99.95],0,F,2.81,0.0,0.0,468|2.81|0.0|0.0,63|2.61|0.0|0.0,796|1.63|0.0|0.0,[]'
    Returns:

    '''
    num_dims = bro_config_dict.value['num_dims']
    cell_num = bro_config_dict.value['cell_num']
    smallRange = bro_config_dict.value['smallRange']
    cell_support = bro_cell_support.value

    pointId, crds, corePartitionId, cur_type, cur_kdist, cur_lrd, cur_lof, knn_info_string, whose_support = knn_info
    # 计算 cellStoreId
    cellStoreId = CellStore.compute_cell_store_id_from_float(crds, num_dims, cell_num, smallRange)
    # 将该点的支持分区 ID 添加至 partitionToCheck 集合
    support_info = []
    if cellStoreId in cell_support.keys():
        whose_support = cell_support[cellStoreId]
        for i in range(1, len(whose_support)):
            current = [whose_support[i], pointId, crds[0], crds[1], 'S', cur_type, cur_kdist, cur_lrd]  # 去掉None
            support_info.append(current)
    else:
        whose_support = [None]
    core_data_set = [corePartitionId, pointId, crds, 'C', cur_type, cur_kdist, cur_lrd, cur_lof, knn_info_string,
                     whose_support]
    return  support_info,core_data_set
#6.2 cal_final_knn
def cal_final_knn(partition_info,bro_config_dict):
    '''
    Args:
        partition_info: key-partition_id ,value-被支持分区，核心分区
        eg:[(1, [(711, 12.38, 1.32, 'S', 'O', 6.67, 0.1748) +[908, [10.96, 99.95], 'C', 'F', 2.81, ['468|2.81|0.0|0.0', '63|2.61|0.0|0.0', '796|1.63|0.0|0.0'], [None, 1]]

    Returns:info_of_mtcobj ：['point_id,crds,partition_id',ksidt,whosesupport,knn_info...]
            eg:['925,[5.68, 0.67],0,6.51,[None],549|6.51,74|1.65,795|1.52',...]
    '''
    # import pydevd_pycharm
    # pydevd_pycharm.settrace('172.27.235.19', port=5678, stdoutToServer=True, stderrToServer=True, suspend=True)
    curPartitionId = partition_info[0]
    partition_details = partition_info[1]
    num_dims = bro_config_dict.value['num_dims']
    k_nn = bro_config_dict.value['k_nn']
    sorted_data = []  # 用于存储每个分区内的MetricObject(扩展+支持点),其中：record:[[9.98, 75.08, '2']]
    countSupporting = 0  # 某分区支持点的数量
    core_data = []
    # 1.区分支持点和核心点数据
    support_data = {}
    can_calculate_lrd = {}
    for detail in partition_details:
        if 'S' in detail:  # 被支持分区，(711, 12.38, 1.32, 'S', 'O', 6.67, 0.1748)
            point_id, point_x, point_y, role, cur_type, k_dist, lrd = detail
            mo = MetricObject(partition_id=curPartitionId, obj=[point_x, point_y, point_id], type=cur_type,
                              role='S',
                              kdist=k_dist, lrd=lrd)
            sorted_data.append(mo)
            support_data[point_id] = mo
            countSupporting += 1
        else:  # 核心分区内的数据(非'O'状态下的数据:F/L)
            # print(detail)
            # [908, [10.96, 99.95],'F', 2.81, 0.0,0.0,'468|2.81|0.0|0.0,63|2.61|0.0|0.0,796|1.63|0.0|0.0', [None, 1]]

            point_id, crds, role, cur_type, k_dist, lrd, lof, knn_info, support_set = detail
            knn_info_dict = {}
            for item in knn_info.split(','):
                parts = item.strip().split('|')#['652', '7.95', '[ -2.5281   502.459875]', '0.0', '0.0']
                key = parts[0]
                temp_dist = float(parts[1])
                cf =np.array(list(map(float, parts[2].strip("[]").split())))
                k_dist = float(parts[-2])
                lrd = float(parts[-1])
                #values = list(map(float, parts[1:]))
                values = [temp_dist,cf,k_dist,lrd]
                knn_info_dict[key] = values
            mo = MetricObject(partition_id=curPartitionId, obj=[crds[0], crds[1], point_id], type=cur_type,
                              role='C',
                              kdist=k_dist, lrd=lrd, lof=lof, knnInDetail=knn_info_dict, whoseSupport=support_set)
            core_data.append(mo)
            if cur_type == 'F':
                sorted_data.append(mo)
            else:  # 'L'
                can_calculate_lrd[mo.get_obj()[-1]] = mo
    # 2. 根据与种子点的距离，排序sorted_data
    central_pivot = sorted_data[0].get_obj()[:-1]
    metric = LFMetric()
    for metric_object in sorted_data:
        metric_object.set_dist_to_pivot(metric.dist(central_pivot, metric_object.get_obj()[:-1])[0])
    sorted_data = sorted(sorted_data, key=lambda obj: obj.get_dist_to_pivot(), reverse=True)  # 降序排序
    # 3. 为分区内的扩展点寻找真实的knn
    # can_calculate_kdist ={}

    for i in range(len(sorted_data)):
        o_R = sorted_data[i]
        o_R = find_expand_KNN_for_single_object(o_R, i, sorted_data, metric, k_nn)
        sec_compute_lcf(o_R, can_calculate_lrd)

    # calculate LOF for those that can calculate LRD
    can_calculate_nof = {}  # type: dict[int, MetricObject]
    sec_compute_nof(k_nn, can_calculate_lrd, support_data, can_calculate_nof)
    # output records
    sec_data_info = output_data_info(can_calculate_lrd, core_data)
    return sec_data_info
#6.2.1 find_expand_KNN_for_single_object
def find_expand_KNN_for_single_object(o_R, current_index, sorted_data, metric, k):
    '''
    在扩展分区内为扩展点查询真实的knn
    Args:
        o_R:
        current_index:
        sorted_data:
        metric:
        k:

    Returns:

    '''
    # import pydevd_pycharm
    # pydevd_pycharm.settrace('172.27.235.19', port=5678, stdoutToServer=True, stderrToServer=True, suspend=True)
    pq = PriorityQueue()  # 优先队列，用来存储 K 个最近邻
    theta = float('inf')  # 阈值，表示最远的距离
    for rid, info in o_R.get_knn_in_detail().items():
        pq.insert(int(rid), info)
        theta = pq.get_priority()  # 获取最大优先级

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
            tempKdist = o_S.get_kdist() if o_S.get_kdist() else 0.0
            tempLrd = o_S.get_lrd() if o_S.get_lrd() else 0.0
            if pq.size() < k and o_S.get_role() == 'S':
                pq.insert(int(o_S.get_obj()[-1]), [dist, cf,tempKdist, tempLrd])
                theta = pq.get_priority()
            elif dist < theta and o_S.get_role() == 'S':
                pq.pop()
                pq.insert(int(o_S.get_obj()[-1]), [dist, cf,tempKdist, tempLrd])
                theta = pq.get_priority()
            inc_current += 1
            i = abs(o_R.get_dist_to_pivot() - o_S.get_dist_to_pivot())
        else:
            o_S = sorted_data[dec_current]
            dist,cf = metric.dist(o_R.get_obj()[:-1], o_S.get_obj()[:-1])
            tempKdist = o_S.get_kdist() if o_S.get_kdist() else 0.0
            tempLrd = o_S.get_lrd() if o_S.get_lrd() else 0.0
            if pq.size() < k and o_S.get_role() == 'S':
                pq.insert(int(o_S.get_obj()[-1]), [dist, cf,tempKdist, tempLrd])
                theta = pq.get_priority()
            elif dist < theta and o_S.get_role() == 'S':
                pq.pop()
                pq.insert(int(o_S.get_obj()[-1]), [dist, cf,tempKdist, tempLrd])
                theta = pq.get_priority()
            dec_current -= 1
            j = abs(o_R.get_dist_to_pivot() - o_S.get_dist_to_pivot())

        if i > pq.get_priority() and j > pq.get_priority() and pq.size() == k:
            kNNfound = True

    o_R.set_kdist(pq.get_priority())
    # 存储 KNN 详情
    knn_in_detail = {}  # 字典
    while pq.size() > 0:
        dist, id, info = pq.pop()
        knn_in_detail[id] = info
    o_R.set_knn_in_detail(knn_in_detail)
    return o_R
#6.2.2 sec_compute_lcf
def sec_compute_lcf(o_S,can_calculate_lcf):
    # import pydevd_pycharm
    # pydevd_pycharm.settrace('172.27.235.19', port=5678, stdoutToServer=True, stderrToServer=True, suspend=True)
    lrd_core = 0.0
    can_lrd = True
    lcf = (0.0, 0.0)
    for rid, info in o_S.get_knn_in_detail().items():  # info =[1.5, [3.0375 1.485 ], 1.78, 12.25788373112295]
        cf = info[1]
        lcf += cf
    lrd_core = np.round(np.linalg.norm(lcf, keepdims=True),2)
    o_S.set_lrd(lrd_core[0])
    o_S.set_type('L')
    can_calculate_lcf[o_S.get_obj()[-1]] = o_S
#6.2.3 sec_compute_nof
def sec_compute_nof(knn, can_calculate_lrd, support_data, can_calculate_lof):
    for o_S in can_calculate_lrd.values():
        can_lof = True
        lof_core = 0.0
        if o_S.get_lrd() <= 1e-9:
            lof_core = 0.0
        else:
            for rid, info in o_S.get_knn_in_detail().items():
                temp_lrd = info[-2]
                if temp_lrd > 0.0:
                    lof_core += temp_lrd
                elif rid in can_calculate_lrd:
                    temp_lrd = can_calculate_lrd[rid].get_lrd()
                    if temp_lrd == 0:
                        pass  # lof_core 不变
                    else:
                        lof_core += temp_lrd
                elif rid in support_data and support_data[rid].get_type in ('L',
                                                                            'O'):  # knn in support_data and have k_dist
                    temp_lrd = support_data[rid].get_lrd()
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
            can_calculate_lof[o_S.get_obj()[-1]] = o_S  # 推测补全

'''
7. cal nof
'''
def compute_nof(support_data_plan_rdd,knn_dist_second_rdd,sec_unfinished_rdd,bro_config_dict):
    '''

    Args:
        support_data_plan_rdd:
        knn_dist_second_rdd:
        sec_unfinished_rdd:
        bro_config_dict:

    Returns:nof_rdd length is:140,details is [['777', [0.96, 58.16], 0, 'O', 0.0, 0.0, 0.0,
    '153|3.55|[  9.45 -43.73]|0.0|0.0,273|3.12|[-27.74  12.46]|0.0|0.0,952|2.54|[-6.26 15.16]|0.0|0.0', [None, 1, 2, 3]],

    '''
    print("7.1. collect kdist for each partition...")
    support1_nof_rdd = support_data_plan_rdd.filter(lambda x: x[0]).flatMap(lambda x: x[0]).filter(
        lambda x: x[5] == 'O').map(lambda y: (y[0], (y[1], y[2], y[3], y[4], y[5], y[6], y[7])))
    # support1_nof_rdd.cache()
    print(f"support1_lrd_rdd length is:{support1_nof_rdd.count()},details is {support1_nof_rdd.take(2)}")#[(1, ('361', 5.41, 56.89, 'S', 'O', 2.94, 45.6)), (2, ('361', 5.41, 56.89, 'S', 'O', 2.94, 45.6))]
    support2_nof_rdd = knn_dist_second_rdd.map(dealEachData).filter(lambda x: x).flatMap(lambda x: x).map(
        lambda y: (y[0], (y[1], y[2], y[3], y[4], y[5], y[6], y[7])))
    # support2_nof_rdd.cache()
    print(f"support2_nof_rdd length is:{support2_nof_rdd.count()},details is {support2_nof_rdd.take(2)}")
    support_nof_info_rdd = support1_nof_rdd.union(support2_nof_rdd)
    print(
        f"support_nof_info_rdd length is:{support_nof_info_rdd.count()},details is {support_nof_info_rdd.take(2)}")#[(1, ('361', 5.41, 56.89, 'S', 'O', 2.94, 45.6)), (2, ('361', 5.41, 56.89, 'S', 'O', 2.94, 45.6))]
    core_sec_unfinished_rdd = sec_unfinished_rdd.map(lambda x: (x[2], (x[0], x[1], x[3], x[4], x[5], x[6], x[7], x[8])))
    print(
        f"core_sec_unfinished_rdd length is:{core_sec_unfinished_rdd.count()},details is {core_sec_unfinished_rdd.take(2)}")
    print("--" * 50)

    # 7.2. callrd-reduce
    # 计算lrd
    # [(925, 0, 0.17931858936043035, [None], '549|6.51,74|1.65,795|1.52'),...]
    print("7.2. Cal nof for each data...")
    kdist_partition_rdd = support_nof_info_rdd.union(core_sec_unfinished_rdd).groupByKey()  # 合并上述信息
    print(f"kdist_partition_rdd is:{kdist_partition_rdd.count()},details is {kdist_partition_rdd.take(2)}")#[(0, <pyspark.resultiterable.ResultIterable object at 0x7527fa3daa90>), (1, <pyspark.resultiterable.ResultIterable object at 0x7527fa3da6d0>)]
    calDatasetNof_with_config = partial(calDatasetNof, bro_config_dict=bro_config_dict)
    nof_rdd = kdist_partition_rdd.flatMap(calDatasetNof_with_config)
    print(f"nof_rdd length is:{nof_rdd.count()},details is {nof_rdd.take(2)}")

    return nof_rdd
#7.1 dealEachData
def dealEachData(data_info):
    '''
    根据每条数据的knn等信息，得到特定分区以及支持该分区的数据kdist信息
    Args:
        data_info: '908,[10.96, 99.95],0,O,2.81,0.3135,1.0125,[None,1]'
                    unfinished = '908,[10.96, 99.95],0,T/L,2.81,0,468|2.81|0.0|0.0,63|2.61|0.0|0.0,796|1.63|0.0|0.0,[None,1]
    Returns:core_partition_info,support_partition_info_set
                [([1, 925, 'C', 0.17931858936043035, '549|6.51,74|1.65,795|1.52', [None]], 、
                [[(1, (711, 'S', 0.17482517482517484)), (1, (691, 'S', 0.2657218777679362))])

    '''

    # data_info='908,[10.96, 99.95],0,F,2.81,0,468|2.81|0.0|0.0,63|2.61|0.0|0.0,796|1.63|0.0|0.0,[None, 0]'
    # print(data_info)
    pointId, crds, role, cur_type, cur_kdist, cur_lof, cur_lrd, knn_info_string, whose_support = data_info
    support_info = []

    for i in range(1, len(whose_support)):
        current = [whose_support[i], pointId, crds[0], crds[1], 'S', cur_type, cur_kdist, cur_lrd]  # 去掉None
        support_info.append(current)
    # output
    # core_partition_info = [corePartitionId,pointId,crds,'C',cur_type, cur_kdist, cur_lrd,cur_lof, knn_info_string, whose_support]
    return support_info
#7.2 calDatasetLrd
def calDatasetNof(kdist_partition,bro_config_dict):
    # ['908', [10.96, 99.95], 0, 'L', 2.81, 0.0, 1.0125,'468|2.81|2.92|0.3275,63|2.61|3.73|0.3077,796|1.63|2.92|0.3171', [None, 1]
    # ('908', 10.96, 99.95, 'S', 'F', 2.81, 0.0)
    curPartitionId = kdist_partition[0]
    kdist_info = kdist_partition[1]
    core_data = []  # 用列表替代 Vector
    support_data = []  # 用列表替代 Vector
    can_calculate_lrd = {}
    k_nn = bro_config_dict.value['k_nn']
    # 解析每个 value
    for detail in kdist_info:
        if 'S' in detail:  # (115, 12.23, 18.79, 'S', 'O', 3.84, 0.2817)
            point_id, point_x, point_y, role, cur_type, k_dist, lrd = detail
            mo = MetricObject(partition_id=curPartitionId, obj=[point_x, point_y, point_id], type=cur_type,
                              role='S',
                              kdist=k_dist, lrd=lrd)
            support_data.append(mo)
        else:  # 核心分区内的数据(非'O'状态下的数据:T/L
            # [908, [10.96, 99.95], 'C', 'T', 2.81, 0,['468|2.81|0.0|0.0', '63|2.61|0.0|0.0', '796|1.63|0.0|0.0'], [None, 1]]
            point_id, crds, role, cur_type, k_dist, lrd, knn_info, support_set = detail
            knn_info_dict = {}
            for item in knn_info.split(','):
                parts = item.strip().split('|')  # ['652', '7.95', '[ -2.5281   502.459875]', '0.0', '0.0']
                key = parts[0]
                temp_dist = float(parts[1])
                cf = np.array(list(map(float, parts[2].strip("[]").split())))
                k_dist = float(parts[-2])
                lrd = float(parts[-1])
                # values = list(map(float, parts[1:]))
                values = [temp_dist, cf, k_dist, lrd]
                knn_info_dict[key] = values
            mo = MetricObject(partition_id=curPartitionId, obj=[crds[0], crds[1], point_id], type=cur_type,
                              role='C',
                              kdist=k_dist, lrd=lrd, knnInDetail=knn_info_dict, whoseSupport=support_set)
            core_data.append(mo)
    third_compute_nof(k_nn,core_data, support_data)
    # output records
    third_data_info = output_data_info(can_calculate_lrd, core_data)
    return third_data_info
#7.2.1 third_compute_nof
def third_compute_nof(knn, core_data, support_data):
    for o_S in core_data:
        can_lof = True
        lof_core = 0.0
        if o_S.get_lrd() <= 1e-9:
            lof_core = 0.0
        else:
            for rid, info in o_S.get_knn_in_detail().items():
                temp_lrd = info[-2]
                if temp_lrd > 0.0:
                    lof_core += temp_lrd

                elif rid in support_data and support_data[rid].get_type in ('L',
                                                                            'O'):  # knn in support_data and have k_dist
                    temp_lrd = support_data[rid].get_lrd()
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



