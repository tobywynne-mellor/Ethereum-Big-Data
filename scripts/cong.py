
from mrjob.job import MRJob
from mrjob.step import MRStep


class PartB(MRJob):
    def mapper_repartition_init(self, _, row):
        try:
            splits = row.split(',')
            if len(splits) == 7:
                # this is a transaction
                to_address = splits[2]
                value = int(splits[3])
                if value > 0:
                    yield(to_address, ("tsc", value))
            elif len(splits) == 5:
                # this is a smart contract
                sc_address = splits[0]
                yield(sc_address, ("sc", 1))
            else:
                pass
        except:
            pass

    def combiner_repartition_init(self, address, values):
        values = [x for x in values]
        data_types = {}
        for arr in values:
            key = arr[0]
            vals = [arr[1]]
            if key in data_types:
                data_types[key] = [sum(v)
                                   for v in zip(data_types[key], vals)]
            else:
                data_types[key] = vals
        for data_type in data_types.items():
            yield(address, (data_type[0], data_type[1].pop()))

    def reducer_repartition_init(self, address, values):
        has_sc = False
        transacted_amount = 0
        values = [x for x in values]
        # loop through the values and count the transacted amounts in smart contracts
        for value in values:
            if value[0] == "tsc":
                transacted_amount += value[1]
            elif value[0] == "sc" and has_sc is False:
                if value[1] > 0:
                    has_sc = True
        # only yield if this is a smart contract
        if has_sc is True:
            yield(address, transacted_amount)

    def mapper_aggregate(self, address, ts_amount):
        yield(None, (address, ts_amount))

    def combiner_aggregate(self, _, values):
        i = 0
        sorted_values = sorted(values, reverse=True, key=lambda tup: tup[1])
        for value in sorted_values:
            yield(None, value)
            i += 1
            if i >= 10:
                break

    def reducer_aggregate(self, _, values):
        i = 0
        sorted_values = sorted(values, reverse=True, key=lambda tup: tup[1])
        for value in sorted_values:
            i += 1
            yield("{} - {} - {}".format(i, value[0], value[1]), None)
            if i >= 10:
                break

    def steps(self):
        return [MRStep(mapper=self.mapper_repartition_init,
                       combiner=self.combiner_repartition_init, reducer=self.reducer_repartition_init),
                MRStep(mapper=self.mapper_aggregate,
                       combiner=self.combiner_aggregate, reducer=self.reducer_aggregate)]


if __name__ == "__main__":
    PartB.JOBCONF = {'mapreduce.job.reduces': '4'}
    PartB.run()
