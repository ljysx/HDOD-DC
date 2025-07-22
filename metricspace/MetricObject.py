class MetricObject:
    def __init__(self, partition_id=None, obj=None, type=None, role=None, kdist=0.0, lrd=0.0, lof=0.0,
                 knnInDetail=None, knnsInString="", whoseSupport=[None], canPrune=False, canCalculateLof=False,
                 distToPivot=float('inf')):
        if whoseSupport is None:
            whoseSupport = [None]
        self.partition_id = partition_id
        self.obj = obj
        self.type = type
        self.role = role
        self.distToPivot = distToPivot
        self.knnInDetail = knnInDetail if knnInDetail is not None else {}#字典
        self.kdist = kdist
        self.lrd = lrd
        self.lof = lof
        self.knnsInString = knnsInString
        self.whoseSupport = whoseSupport
        self.canPrune = canPrune
        self.canCalculateLof = canCalculateLof
        self.nearestNeighborDist = float('inf')

    def __str__(self):
        sb = []
        sb.append(f",type: {self.type}")
        sb.append(f",in Partition: {self.partition_id}")
        sb.append(", Knn in detail: ")
        for k, v in self.knnInDetail.items():
            sb.append(f",{k},{v}")
        return ''.join(sb)

    def compare_to(self, other):
        if not isinstance(other, MetricObject):
            raise TypeError(f"Cannot compare {type(other)} with MetricObject")
        if other.distToPivot > self.distToPivot:
            return 1
        elif other.distToPivot < self.distToPivot:
            return -1
        else:
            return 0

    # Getters and Setters (properties)
    def get_partition_id(self):
        return self.partition_id

    def set_partition_id(self, partition_id):
        self.partition_id = partition_id

    def get_type(self):
        return self.type

    def set_type(self, type):
        self.type = type

    def get_role(self):
        return self.role

    def set_role(self, role):
        self.role = role
    def get_dist_to_pivot(self):
        return self.distToPivot

    def set_dist_to_pivot(self, distToPivot):
        self.distToPivot = distToPivot

    def get_obj(self):
        return self.obj

    def set_obj(self, obj):
        self.obj = obj

    def get_whose_support(self):
        return self.whoseSupport

    def set_whose_support(self, whoseSupport):
        self.whoseSupport = whoseSupport

    def get_knn_in_detail(self):
        return self.knnInDetail

    def set_knn_in_detail(self, knnInDetail):
        self.knnInDetail = knnInDetail

    def get_kdist(self):
        return self.kdist

    def set_kdist(self, kdist):
        self.kdist = kdist

    def get_lrd(self):
        return self.lrd

    def set_lrd(self, lrd):
        self.lrd = lrd

    def get_lof(self):
        return self.lof

    def set_lof(self, lof):
        self.lof = lof

    def get_nearest_neighbor_dist(self):
        return self.nearestNeighborDist

    def set_nearest_neighbor_dist(self, nearestNeighborDist):
        self.nearestNeighborDist = nearestNeighborDist

    def get_can_prune(self):
        return self.canPrune

    def set_can_prune(self, canPrune):
        self.canPrune = canPrune

    def get_can_calculate_lof(self):
        return self.canCalculateLof

    def set_can_calculate_lof(self, canCalculateLof):
        self.canCalculateLof = canCalculateLof

    def get_knns_in_string(self):
        return self.knnsInString

    def set_knns_in_string(self, knnsInString):
        self.knnsInString = knnsInString

    def get_expand_dist(self):
        return self.expandDist

    def set_expand_dist(self, expandDist):
        self.expandDist = expandDist
