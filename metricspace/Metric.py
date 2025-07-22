# metric.py
import numpy as np


class IMetric:
    def __init__(self):
        self.num_of_dist_comp = 0  # 距离计算次数计数器

    def dist(self, o1, o2):
        raise NotImplementedError("Subclasses should implement this method.")

    def get_num_of_dist_comp(self):
        return self.num_of_dist_comp

class LFMetric(IMetric):
    def dist(self, o1, o2):
        self.num_of_dist_comp += 1
        return self.comp_lf_dist(o1, o2)

    @staticmethod
    def comp_lf_dist(o1, o2):
        # 计算 L1 距离
        moe = round((sum((v1 - v2) ** 2 for v1, v2 in zip(o1, o2))) ** 0.5,2)
        cf = (np.array(o1) - np.array(o2)) * (moe ** 2)  # OF（Xj,Xi)库仑力：|||Xj-Xi|**(m-2)(Xj-Xi)
        cf = np.round(cf,4)
        return moe,cf

    def get_num_of_dist_comp(self):
        return self.num_of_dist_comp
class L1Metric(IMetric):
    """
    L1 distance: the sum of |v1[i] - v2[i]| for any i
    """

    def dist(self, o1, o2):
        self.num_of_dist_comp += 1
        return self.comp_l1_dist(o1, o2)

    @staticmethod
    def comp_l1_dist(o1, o2):
        # 计算 L1 距离
        return round(sum(abs(v1 - v2) for v1, v2 in zip(o1, o2)),2)

    def get_num_of_dist_comp(self):
        return self.num_of_dist_comp


class L2Metric(IMetric):
    """
    L2 distance: the sqrt of sum (v1[i] - v2[i])*(v1[i] - v2[i]) for any i
    """

    def dist(self, o1, o2):
        self.num_of_dist_comp += 1
        return self.comp_l2_dist(o1, o2)

    @staticmethod
    def comp_l2_dist(o1, o2):
        # 计算 L2 距离
        return round((sum((v1 - v2) ** 2 for v1, v2 in zip(o1, o2))) ** 0.5,2)

    def get_num_of_dist_comp(self):
        return self.num_of_dist_comp
