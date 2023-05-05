# import MRJob class from the mrjob library
from mrjob.job import MRJob
import re
from mrjob.protocol import RawValueProtocol


class MRProcessDataSet(MRJob):
    # output protocol should be RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol
    # define the mapper function
    def mapper(self, _, line):
        line = line.strip()
        Items_regex = r'"id":(\d+),.*?"title":"(.*?)","year":(\d+),"n_citation":(\d+),"page_start":"(.*?)","page_end":"(.*?)"'

        # search for a match between the regular expression pattern and the line
        match = re.search(Items_regex, line)
        if match:
            # extract the groups from the match object and assign them to variables
            id_ = match.group(1)
            title = match.group(2)
            year = match.group(3)
            n_citation = match.group(4)
            page_start = match.group(5)
            page_end = match.group(6)

            # yield a key-value pair of None and a string with tab-separated values
            yield None, f"{id_}\t{title}\t{year}\t{n_citation}\t{page_start}\t{page_end}"


if __name__ == '__main__':
    MRProcessDataSet.run()
