import math


class CellStore:
    def __init__(self, cellStoreId=None, core_partition_id=None):
        self.cellStoreId = cellStoreId
        self.core_partition_id = core_partition_id if core_partition_id is not None else float('-inf')
        self.support_partition_id = set()

    def print_cell_store_basic(self):
        return f"{self.cellStoreId}, C:{self.core_partition_id}"

    def print_cell_store_with_support(self):
        str_repr = f"{self.cellStoreId}, C:{self.core_partition_id}, S:"
        for key in self.support_partition_id:
            str_repr += str(key) + ","
        return str_repr.rstrip(',')

    @staticmethod
    def compute_cell_store_id(data, dim, numCellPerDim):
        #print(data,len(data))
        # if len(data) != dim:
        #     return -1
        cellId = 0
        for i in range(dim):
            cellId += int(data[i] * math.pow(numCellPerDim, i))
        return cellId

    @staticmethod
    def compute_cell_store_id_from_string(dataString, dim, numCellPerDim):
        data = dataString.split(",")
        #print(data,len(data))
        # if len(data) != dim:
        #     return -1
        cellId = 0
        for i in range(dim):
            cellId += int(float(data[i]) * math.pow(numCellPerDim, i))
        return cellId

    @staticmethod
    def compute_cell_store_id_from_float(data, dim, numCellPerDim, smallRange):
        # if len(data) != dim:
        #     return -1
        cellId = 0
        for i in range(dim):
            tempIndex = int(math.floor(data[i] / smallRange))
            if tempIndex == numCellPerDim:# "1" assigned to 49
                tempIndex-=1
            cellId += int(tempIndex * math.pow(numCellPerDim, i))
        return cellId

    def generate_cell_coordinate(self, cellIdNum, dim, numCellPerDim):
        cellCoorIndex = [0] * dim
        for i in range(dim - 1, -1, -1):
            divider = int(math.pow(numCellPerDim, i))
            countPerDim = cellIdNum // divider
            cellCoorIndex[i] = countPerDim
            cellIdNum -= countPerDim * divider
        return cellCoorIndex



