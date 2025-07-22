import heapq

class PriorityQueue:
    SORT_ORDER_ASCENDING = True
    SORT_ORDER_DESCENDING = False

    def __init__(self, values=None):
       # self.sort_order = sort_order
        #self.values = []  # 存储值的列表
        #self.priorities = []  # 存储优先级的列表
        self.values=[]
        #self.capacity = initial_capacity

    def sorts_earlier_than(self, p1, p2):
        if self.sort_order == self.SORT_ORDER_ASCENDING:
            return p1 < p2
        return p2 < p1

    def insert(self, value, priority):
        # 插入一个新元素，使用最大堆堆插入并维持堆的结构
        heapq.heappush(self.values, (-priority[0],value,priority))
        #self.priorities.append(priority)

    def size(self):
        return len(self.values)

    def clear(self):
        self.values.clear()
        #self.priorities.clear()

    def reset(self):
        self.values = []
        #self.priorities = []

    def get_value(self):
        return self.values[0][1] if self.values else None

    def get_priority(self):
        return -self.values[0][0] if self.values else float('inf')

    #def demote(self, index, value, priority):
        # 此方法用于将元素下沉以维护堆的结构
        #heapq.heappush(self.values, (priority, value))

    def pop(self):
        if self.size() == 0:
            raise IndexError('pop from empty priority queue')
        priority_dist,value,priority = heapq.heappop(self.values)
        #self.priorities.remove(priority)
        return -priority_dist,value,priority

    def set_sort_order(self, sort_order):
        if self.sort_order != sort_order:
            self.sort_order = sort_order
            self._reheapify()

    def _reheapify(self):
        # 重新堆化所有元素
        self.values = [(priority, value) for priority, value in zip(self.priorities, self.values)]
        heapq.heapify(self.values)

    def check(self):
        # 用于检查堆的正确性，这里仅为示例，通常不需要实现
        for i in range(len(self.values) // 2):
            left_child = 2 * i + 1
            right_child = 2 * i + 2
            if left_child < len(self.values):
                assert self.sorts_earlier_than(self.values[left_child][0], self.values[i][0]), "Heap invariant violated"
            if right_child < len(self.values):
                assert self.sorts_earlier_than(self.values[right_child][0], self.values[i][0]), "Heap invariant violated"
