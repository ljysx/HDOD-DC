from pyspark import SparkContext
import math


class PartitionPlan:
    def __init__(self):
        # 数据点存储（转换为 Python 字典）
        self.data_points = {}
        self.num_buckets_dim = 0#桶的数量
        self.num_dim = 0
        self.num_data = 0#桶内点的数量
        self.start_domain = []
        self.end_domain = []
        self.map_num = 0 #:cell_num
        self.small_domain = 2#:small_range ‘4，6’：2  4/2=2

    def get_num_data(self):
        return self.num_data

    def setup_partition_plan(self, num_dim, num_data, num_buckets_dim, start_domain, end_domain, map_num, small_domain):
        self.num_dim = num_dim
        self.num_data = num_data
        self.num_buckets_dim = num_buckets_dim
        self.start_domain = start_domain
        self.end_domain = end_domain
        self.map_num = map_num
        self.small_domain = small_domain
        self.data_points = {}

    def add_data_points(self, points):
        self.data_points = points

    def check_elements_in_frequencies(self, frequencies, index_i):
        return any(frequencies[i] != 0 for i in range(index_i, len(frequencies)))

    def separate_partitions(self, by_dim, K):
        new_partitions = [PartitionPlan() for _ in range(self.num_buckets_dim)]
        each_part_num = self.num_data / self.num_buckets_dim
        each_part_num = max(K, each_part_num)
        frequencies = [0] * (self.map_num+1)


        # Load data into frequency array
        for key, value in self.data_points.items():
            index_of_key = int(math.floor(float(key.split(",")[by_dim]) / self.small_domain))
            frequencies[index_of_key] += value


        index_i = 0
        #1.from 0 to num_buckets_dim -2
        for i in range(self.num_buckets_dim - 1):
            print("i:",i)
            temp_sum = 0
            new_start_point = index_i * self.small_domain
            check_if_elements = True
            while temp_sum < each_part_num:
                if not self.check_elements_in_frequencies(frequencies, index_i):
                    check_if_elements = False
                    break
                elif temp_sum > K and temp_sum > 0.5 * each_part_num and temp_sum + frequencies[
                    index_i] > 2 * each_part_num:
                    break
                else:
                    temp_sum += frequencies[index_i]
                    index_i += 1

            # if not check_if_elements and temp_sum == 0:
            #     break

            new_end_point = index_i * self.small_domain
            new_data_points = {}
            new_num_data = 0
            # Add points to new Data Points
            for key, value in self.data_points.items():
                if float(key.split(",")[by_dim]) >= new_start_point and float(key.split(",")[by_dim]) < new_end_point:
                    new_data_points[key] = value
                    new_num_data += value
            # Update the domain range
            new_start_domain = self.start_domain[:]
            new_end_domain = self.end_domain[:]
            new_start_domain[by_dim] = new_start_point
            new_end_domain[by_dim] = new_end_point
            # Set up new partition plan
            new_partitions[i].setup_partition_plan(self.num_dim, new_num_data, self.num_buckets_dim, new_start_domain,
                                         new_end_domain, self.map_num, self.small_domain)
            new_partitions[i].add_data_points(new_data_points)
            #print(new_partitions[i].get_start_and_end())

        # 2.Final partition handling
        print("i",i)
        new_start_domain = self.start_domain[:]
        new_end_domain = self.end_domain[:]
        new_start_point = index_i * self.small_domain
        new_start_domain[by_dim] = new_start_point
        new_data_points = {}
        new_num_data = 0
        for key, value in self.data_points.items():
            if float(key.split(",")[by_dim]) >= new_start_point and float(key.split(",")[by_dim]) < self.end_domain[
                by_dim]:
                new_data_points[key] = value
                new_num_data += value
        if new_num_data < K and i >= 0:
            final_new_num_data = new_num_data + new_partitions[i].num_data
            new_start_domain[by_dim] = new_partitions[i].start_domain[by_dim]
            new_data_points.update(new_partitions[i].data_points)
            new_partitions[i].setup_partition_plan(self.num_dim, final_new_num_data, self.num_buckets_dim,
                                                    new_start_domain, new_end_domain, self.map_num, self.small_domain)
            new_partitions[i].add_data_points(new_data_points)
            #print("a",new_partitions[i].get_start_and_end())
        else:

            new_partitions[i+1].setup_partition_plan(self.num_dim, new_num_data, self.num_buckets_dim, new_start_domain,
                                                    new_end_domain, self.map_num, self.small_domain)
            new_partitions[i+1].add_data_points(new_data_points)
            #print("b",new_partitions[i+1].get_start_and_end())


        return new_partitions

    def get_start_and_end(self):
        return ','.join([f"{start},{end}" for start, end in zip(self.start_domain, self.end_domain)])

    def print_partition_plan(self):
        print(f"Number of points in this partition: {self.num_data}")
        print("Data points list:")
        for key, value in self.data_points.items():
            print(f"{key} -> {value}")
        print(f"Number of dimensions: {self.num_dim}")
        print(f"Number of desired buckets per dimension: {self.num_buckets_dim}")
        print("Start points and end points:")
        for start, end in zip(self.start_domain, self.end_domain):
            print(f"Start from {start}   end to {end}")
        print("----------------------------------------------------------------------------")


# Initialize SparkContext
# sc = SparkContext(appName="PartitionPlanExample")


# Define main function to run Spark
def run_spark():
    num_dim = 3
    num_buckets_dim = 5
    num_data = 1000
    map_num = 10
    small_domain = 2
    start_domain = [0.0] * num_dim
    end_domain = [10.0] * num_dim

    # Initialize PartitionPlan
    pp = PartitionPlan()
    pp.setup_partition_plan(num_dim, num_data, num_buckets_dim, start_domain, end_domain, map_num, small_domain)

    # Example of adding data points (use a real dataset in actual use case)
    data_points = {"0.5,0.5,0.5": 1, "1.5,1.5,1.5": 2, "2.5,2.5,2.5": 3}
    pp.add_data_points(data_points)

    # Run partitioning (simulate separation by dimension 0, with K=3)
    new_partitions = pp.separate_partitions(0, 3)

    # Print partition plans
    for partition in new_partitions:
        partition.print_partition_plan()


# if __name__ == "__main__":
#     run_spark()
